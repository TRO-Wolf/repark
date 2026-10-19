"""R-TPCDS-HARNESS — SF0.01 facade oracle battery (DuckDB differential).

**Always-on:** an explicit curated list of ≥10 query ids (deterministic — not
"first 10 runnable"). Each is pinned against the SF1 status ledger (OK → DuckDB
diff; ERROR → EXPECTED-ERROR class; TIMEOUT → correctness still required at
SF0.01).

**Full 99:** env-gated behind ``REPARK_TPCDS_FULL=1``.

DuckDB is hard-provisioned in the root ``dev`` group (``duckdb==1.5.5``) so the
gate venv runs this battery; ``importorskip`` remains so a bare venv degrades to
skip, never failure. ``importorskip`` is ONLY for missing duckdb/extension —
EXPECTED-ERROR pins must never skip.

Never touches AWS. D1: parquet temp views only.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

duckdb = pytest.importorskip("duckdb")

from repark import ReparkSession  # noqa: E402

# Harness imports (bench/ is not under repark_parity/src — load as package)
_TPCDS_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench" / "tpcds"
_LEDGER_PATH = _TPCDS_DIR / "sf1_status_ledger.json"

# Explicit curated SF0.01 smoke pins (≥10). Chosen for cheap SF0.01 walls and stable
# DuckDB-diff — not "first N runnable". Keep sorted for readability; order of
# parametrization is this list order.
CURATED_SMOKE_QUERY_IDS: tuple[int, ...] = (
    3,
    5,  # D2 SparkConcat canary (was Schema ERROR on concat Utf8View)
    6,
    7,
    12,
    15,
    19,
    42,
    52,
    55,
    80,  # D2 SparkConcat canary (was Schema ERROR on concat Utf8View)
    82,
    84,  # D2 SparkConcat canary (was Schema ERROR on concat Utf8View)
    91,
    96,
)


def _load_tpcds_package() -> Any:
    import importlib
    import types

    package_name = "repark_tpcds_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_TPCDS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.runner")


def _require_tpcds_extension() -> None:
    """INSTALL+LOAD tpcds; skip loudly if the extension repo is unreachable."""
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute("INSTALL tpcds")
        connection.execute("LOAD tpcds")
    except Exception as exc:
        pytest.skip(f"DuckDB tpcds extension unavailable (INSTALL tpcds failed): {exc}")
    finally:
        connection.close()


def _load_sf1_ledger() -> dict[str, dict[str, str]]:
    """Return the per-query map from the SF1 ledger (handles nested provenance schema)."""
    if not _LEDGER_PATH.is_file():
        pytest.fail(
            f"SF1 status ledger missing at {_LEDGER_PATH}; "
            "run D1 scoreboard (run_tpcds.py --sf 1 --ledger …) first"
        )
    payload = json.loads(_LEDGER_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        pytest.fail(f"SF1 ledger must be a JSON object; got {payload!r}")
    if "queries" in payload and isinstance(payload["queries"], dict):
        queries = payload["queries"]
        if payload.get("scale_factor") not in (1, 1.0, "1", "1.0"):
            pytest.fail(f"SF1 ledger scale_factor must be 1; got {payload.get('scale_factor')!r}")
    else:
        queries = payload
    if len(queries) != 99:
        pytest.fail(f"SF1 ledger must map 99 queries; got {len(queries)}")
    return queries


def _collect_repark_rows(frame: Any) -> list[tuple[Any, ...]]:
    """Schema-name ordered rows (matches runner._repark_collect)."""
    if hasattr(frame, "to_arrow"):
        table = frame.to_arrow()
        names = list(table.column_names)
        return [tuple(row[name] for name in names) for row in table.to_pylist()]
    return [tuple(row) for row in frame.collect()]


@pytest.fixture(scope="module")
def tpcds_sf001_paths(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Materialize SF0.01 parquet once per module under a private pytest cache."""
    _require_tpcds_extension()
    _load_tpcds_package()
    import importlib

    datagen = importlib.import_module("repark_tpcds_bench.datagen")
    root = tmp_path_factory.mktemp("tpcds-data")
    return datagen.ensure_parquet_sf(0.01, data_root=root)


@pytest.fixture
def spark_tpcds(tpcds_sf001_paths: Path) -> Any:
    session = ReparkSession.builder.appName("pytest-tpcds-smoke").getOrCreate()
    import importlib

    datagen = importlib.import_module("repark_tpcds_bench.datagen")
    for table_name in datagen.TABLES:
        path = tpcds_sf001_paths / f"{table_name}.parquet"
        session.read.parquet(str(path)).createOrReplaceTempView(table_name)
    yield session
    session.stop()


@pytest.fixture
def duck_tpcds(tpcds_sf001_paths: Path) -> Any:
    _require_tpcds_extension()
    connection = duckdb.connect(database=":memory:")
    import importlib

    datagen = importlib.import_module("repark_tpcds_bench.datagen")
    for table_name in datagen.TABLES:
        path = tpcds_sf001_paths / f"{table_name}.parquet"
        path_sql = str(path).replace("'", "''")
        connection.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path_sql}')"
        )
    yield connection
    connection.close()


def _query_ids_for_param() -> list[int]:
    """Always-on curated set; full 99 when REPARK_TPCDS_FULL=1."""
    if os.environ.get("REPARK_TPCDS_FULL", "").strip() in {"1", "true", "TRUE", "yes"}:
        return list(range(1, 100))
    return list(CURATED_SMOKE_QUERY_IDS)


def test_curated_smoke_list_has_at_least_ten() -> None:
    """HARD: curated always-on list must name ≥10 explicit query ids."""
    assert len(CURATED_SMOKE_QUERY_IDS) >= 10
    assert len(set(CURATED_SMOKE_QUERY_IDS)) == len(CURATED_SMOKE_QUERY_IDS)
    for query_nr in CURATED_SMOKE_QUERY_IDS:
        assert 1 <= query_nr <= 99


def test_curated_smoke_pins_d2_concat_fixed_queries() -> None:
    """D2 R-TPCDS-FIXES: Q5/Q80/Q84 must stay on the always-on smoke list.

    Mutation-proof: removing any of these ids while leaving ≥10 others must fail this pin
    (the length-only check is not enough).
    """
    for query_nr in (5, 80, 84):
        assert query_nr in CURATED_SMOKE_QUERY_IDS, (
            f"Q{query_nr} missing from CURATED_SMOKE_QUERY_IDS (D2 SparkConcat fix pin)"
        )


def test_sf1_ledger_d2_notes_disclose_sf001_evidence() -> None:
    """D2 ERROR→OK flips must not look SF1-board-verified without disclosure.

    Mutation-proof: notes must mention SF0.01 evidence scale (and the three fixed ids).
    """
    payload = json.loads(_LEDGER_PATH.read_text(encoding="utf-8"))
    notes = payload.get("notes")
    assert isinstance(notes, list) and notes, "ledger notes must be a non-empty list"
    joined = " ".join(str(item) for item in notes)
    assert "SF0.01" in joined, f"D2 notes must disclose SF0.01 evidence scale; got {joined!r}"
    # Exact triple (not substring-hollow: ``"Q5" in "Q50/…"`` would pass).
    assert "Q5/Q80/Q84" in joined, (
        f"D2 notes must name the fixed triple Q5/Q80/Q84 exactly; got {joined!r}"
    )
    # Status still OK for the three (scoreboard consumers).
    queries = payload["queries"]
    for query_nr in ("5", "80", "84"):
        assert queries[query_nr]["status"] == "OK", f"Q{query_nr} expected OK after D2"


def test_sf1_ledger_covers_all_ninety_nine_queries() -> None:
    """Mutation-proof: ledger must name every TPC-DS query exactly once."""
    ledger = _load_sf1_ledger()
    assert set(ledger.keys()) == {str(number) for number in range(1, 100)}
    for key, entry in ledger.items():
        assert "status" in entry, f"Q{key} missing status"
        assert entry["status"] in {"OK", "WRONG-RESULT", "ERROR", "TIMEOUT", "DIED"}
        if entry["status"] == "ERROR":
            assert entry.get("error_class"), f"Q{key} ERROR requires non-empty error_class"
        if entry["status"] == "TIMEOUT":
            assert entry.get("error_class"), (
                f"Q{key} TIMEOUT requires error_class (Timeout or Slow)"
            )


@pytest.mark.parametrize("query_nr", _query_ids_for_param())
def test_tpcds_sf001_matches_sf1_ledger(
    query_nr: int,
    spark_tpcds: Any,
    duck_tpcds: Any,
) -> None:
    """Pin each Q against the SF1 scoreboard status (OK → diff; ERROR → expected class)."""
    _require_tpcds_extension()
    ledger = _load_sf1_ledger()
    if str(query_nr) not in ledger:
        pytest.fail(f"Q{query_nr} missing from SF1 ledger")
    entry = ledger[str(query_nr)]
    expected_status = entry["status"]

    import importlib

    queries_mod = importlib.import_module("repark_tpcds_bench.queries")
    compare_mod = importlib.import_module("repark_tpcds_bench.compare")
    runner_mod = importlib.import_module("repark_tpcds_bench.runner")

    all_queries = {query.query_nr: query for query in queries_mod.load_queries()}
    query = all_queries[query_nr]
    ordered = query.is_ordered

    if expected_status == "OK":
        duck_rows = list(duck_tpcds.execute(query.original_sql).fetchall())
        repark_rows = _collect_repark_rows(spark_tpcds.sql(query.sql_for_repark))
        result = compare_mod.compare_result_sets(repark_rows, duck_rows, ordered=ordered)
        assert result.equal, (
            f"Q{query_nr} ledger=OK but SF0.01 DuckDB-diff failed: {result.message}"
        )
        return

    if expected_status == "WRONG-RESULT":
        duck_rows = list(duck_tpcds.execute(query.original_sql).fetchall())
        try:
            repark_rows = _collect_repark_rows(spark_tpcds.sql(query.sql_for_repark))
        except Exception as exc:
            pytest.fail(
                f"Q{query_nr} ledger=WRONG-RESULT but repark now raises "
                f"({type(exc).__name__}: {exc}); update sf1_status_ledger.json"
            )
        result = compare_mod.compare_result_sets(repark_rows, duck_rows, ordered=ordered)
        assert not result.equal, (
            f"Q{query_nr} ledger=WRONG-RESULT but SF0.01 now matches DuckDB; "
            "update sf1_status_ledger.json (silent fix)"
        )
        return

    if expected_status == "TIMEOUT":
        # TIMEOUT is scale/machine-dependent — do not require SF0.01 to raise.
        # Still run SF0.01 DuckDB-diff so TIMEOUT cannot escape-hatch wrong results.
        error_class = entry.get("error_class") or ""
        assert error_class in {"Timeout", "Slow"} or error_class.lower().startswith("timeout"), (
            f"Q{query_nr} ledger=TIMEOUT requires error_class Timeout|Slow; got {error_class!r}"
        )
        duck_rows = list(duck_tpcds.execute(query.original_sql).fetchall())
        repark_rows = _collect_repark_rows(spark_tpcds.sql(query.sql_for_repark))
        result = compare_mod.compare_result_sets(repark_rows, duck_rows, ordered=ordered)
        assert result.equal, (
            f"Q{query_nr} ledger=TIMEOUT but SF0.01 DuckDB-diff failed "
            f"(correctness still required at SF0.01): {result.message}"
        )
        return

    if expected_status == "DIED":
        # Process death is host-dependent; SF0.01 still requires correctness if it runs.
        duck_rows = list(duck_tpcds.execute(query.original_sql).fetchall())
        repark_rows = _collect_repark_rows(spark_tpcds.sql(query.sql_for_repark))
        result = compare_mod.compare_result_sets(repark_rows, duck_rows, ordered=ordered)
        assert result.equal, (
            f"Q{query_nr} ledger=DIED but SF0.01 DuckDB-diff failed: {result.message}"
        )
        return

    if expected_status == "ERROR":
        expected_class = entry.get("error_class")
        try:
            repark_frame = spark_tpcds.sql(query.sql_for_repark)
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
