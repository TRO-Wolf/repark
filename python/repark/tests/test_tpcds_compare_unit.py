"""Unit pins for the TPC-DS compare kernel + runner helpers (R-TPCDS-HARNESS / D1).

Mutation-proof: off-by-one large integers must fail; near-equal floats pass;
ORDER BY paths use ordered compare; exit codes and disk gate match the charter.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

_TPCDS_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench" / "tpcds"


def _load_package() -> object:
    package_name = "repark_tpcds_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_TPCDS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.compare")


def _compare_mod() -> object:
    return _load_package()


def _runner_mod() -> object:
    _load_package()
    return importlib.import_module("repark_tpcds_bench.runner")


def _queries_mod() -> object:
    _load_package()
    return importlib.import_module("repark_tpcds_bench.queries")


def _datagen_mod() -> object:
    _load_package()
    return importlib.import_module("repark_tpcds_bench.datagen")


def test_integer_off_by_one_at_million_is_wrong_result() -> None:
    """Relative 1e-6 must NOT mask int off-by-one at |k| >= 1e6."""
    compare = _compare_mod()
    result = compare.compare_result_sets([(6_000_000,)], [(6_000_001,)])
    assert not result.equal, result.message


def test_float_integral_off_by_one_at_million_is_wrong_result() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(6_000_000.0,)], [(6_000_001.0,)])
    assert not result.equal, result.message


def test_decimal_off_by_one_at_million_is_wrong_result() -> None:
    from decimal import Decimal

    compare = _compare_mod()
    result = compare.compare_result_sets(
        [(Decimal("6000000"),)],
        [(Decimal("6000001"),)],
    )
    assert not result.equal, result.message


def test_integer_exact_match_ok() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(6_000_000, "a")], [(6_000_000, "a")])
    assert result.equal, result.message


def test_mixed_int_float_near_equal_uses_tolerance() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(5,)], [(5.0000001,)])
    assert result.equal, result.message


def test_float_relative_tolerance_ok() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(1.0,)], [(1.0 + 1e-7,)])
    assert result.equal, result.message


def test_float_beyond_tolerance_wrong() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(1.0,)], [(1.0 + 1e-4,)])
    assert not result.equal, result.message


def test_null_vs_value_wrong() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(None,)], [(1,)])
    assert not result.equal, result.message


def test_null_vs_null_ok() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(None, 1)], [(None, 1)])
    assert result.equal, result.message


def test_row_count_mismatch() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(1,)], [(1,), (2,)])
    assert not result.equal
    assert "row count" in result.message


def test_unordered_rows_match() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(2, "b"), (1, "a")], [(1, "a"), (2, "b")])
    assert result.equal, result.message


def test_ordered_rows_mismatch_when_order_differs() -> None:
    """ORDER BY path: same multiset but different order is WRONG-RESULT."""
    compare = _compare_mod()
    result = compare.compare_result_sets(
        [(2, "b"), (1, "a")],
        [(1, "a"), (2, "b")],
        ordered=True,
    )
    assert not result.equal, result.message


def test_ordered_rows_match_in_order() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets(
        [(1, "a"), (2, "b")],
        [(1, "a"), (2, "b")],
        ordered=True,
    )
    assert result.equal, result.message


def test_bool_not_equal_to_int_one() -> None:
    compare = _compare_mod()
    result = compare.compare_result_sets([(True,)], [(1,)])
    assert not result.equal, result.message


def test_sql_has_order_by_detection() -> None:
    queries = _queries_mod()
    assert queries.sql_has_order_by("SELECT 1 ORDER BY 1")
    assert queries.sql_has_order_by("select a from t\norder by a desc")
    assert not queries.sql_has_order_by("SELECT 1")
    assert not queries.sql_has_order_by("SELECT order_date FROM t")
    # Literals/comments must not force ordered compare.
    assert not queries.sql_has_order_by("SELECT 'order by x' AS s FROM t")
    assert not queries.sql_has_order_by("SELECT 1 -- order by x\nFROM t")
    assert not queries.sql_has_order_by("SELECT 1 /* order by x */ FROM t")


def test_tables_are_twenty_four() -> None:
    datagen = _datagen_mod()
    assert len(datagen.TABLES) == 24
    assert "store_sales" in datagen.TABLES
    assert "web_sales" in datagen.TABLES


def test_default_data_root_is_private() -> None:
    datagen = _datagen_mod()
    root = datagen.default_data_root()
    assert root.name == "repark-tpcds"
    assert "tmp" not in str(root).split("/")[-2:]  # not sticky /tmp/repark-tpcds


def test_datagen_refuses_nan_scale_factor() -> None:
    import pytest

    datagen = _datagen_mod()
    with pytest.raises(ValueError, match="scale_factor"):
        datagen.ensure_parquet_sf(float("nan"))


def test_datagen_refuses_symlink_cache(tmp_path: Path) -> None:
    import pytest

    datagen = _datagen_mod()
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(ValueError, match="symlink"):
        datagen.ensure_parquet_sf(0.01, data_root=link)


def test_exit_code_priority_wrong_error_timeout() -> None:
    runner = _runner_mod()

    def _q(status: str) -> object:
        return runner.QueryResult(
            query_nr=1,
            status=status,
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
        )

    board = runner.Scoreboard(scale_factor=1.0, data_dir="", environment={})
    board.queries = [_q("OK")]
    assert runner.exit_code_for_board(board) == 0
    board.queries = [_q("TIMEOUT")]
    assert runner.exit_code_for_board(board) == 5
    board.queries = [_q("ERROR"), _q("TIMEOUT")]
    assert runner.exit_code_for_board(board) == 4
    board.queries = [_q("WRONG-RESULT"), _q("ERROR")]
    assert runner.exit_code_for_board(board) == 3


def test_exit_code_died_is_six() -> None:
    runner = _runner_mod()

    def _q(status: str) -> object:
        return runner.QueryResult(
            query_nr=1,
            status=status,
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
        )

    board = runner.Scoreboard(scale_factor=1.0, data_dir="", environment={})
    board.queries = [_q("DIED")]
    assert runner.exit_code_for_board(board) == 6
    board.queries = [_q("DIED"), _q("TIMEOUT")]
    assert runner.exit_code_for_board(board) == 6
    board.queries = [_q("DIED"), _q("WRONG-RESULT")]
    assert runner.exit_code_for_board(board) == 3


def test_exit_code_skipped_empty_is_zero() -> None:
    runner = _runner_mod()
    board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="",
        environment={},
        skipped=True,
        findings=["disk"],
    )
    assert runner.exit_code_for_board(board) == 0


def test_sf_disk_gate_below_threshold_skips(tmp_path: Path, monkeypatch: object) -> None:
    runner = _runner_mod()
    monkeypatch.setattr(runner, "free_disk_gib", lambda _path: 1.0)  # type: ignore[attr-defined]
    ok, free = runner.sf_disk_gate(tmp_path, min_free_gib=5.0)
    assert not ok
    assert free == 1.0


def test_sf_disk_gate_above_threshold_ok(tmp_path: Path, monkeypatch: object) -> None:
    runner = _runner_mod()
    monkeypatch.setattr(runner, "free_disk_gib", lambda _path: 50.0)  # type: ignore[attr-defined]
    ok, free = runner.sf_disk_gate(tmp_path, min_free_gib=5.0)
    assert ok
    assert free == 50.0


def test_classify_error_rollup() -> None:
    runner = _runner_mod()
    error_class, hint = runner.classify_error("Analyzer error: ROLLUP not supported")
    assert error_class == "Rollup"
    assert hint == "ROLLUP"


def test_classify_error_window() -> None:
    runner = _runner_mod()
    error_class, hint = runner.classify_error("not supported: OVER (PARTITION BY x)")
    assert error_class == "Window"
    assert hint is not None


def test_classify_error_exception_is_not_except_setop() -> None:
    """Census pin: 'exception' / PySparkException must not label as EXCEPT set-op."""
    runner = _runner_mod()
    arrow_msg = (
        "PySparkException: External error: Arrow error: Invalid argument error: "
        "column types must match schema types, expected Utf8"
    )
    error_class, hint = runner.classify_error(arrow_msg)
    assert error_class != "SetOp", (error_class, hint)
    assert hint != "EXCEPT", (error_class, hint)
    assert error_class == "Schema", (error_class, hint)

    bare = runner.classify_error("RuntimeError: Exception while planning query")
    assert bare[0] != "SetOp"
    assert bare[1] != "EXCEPT"

    real_except = runner.classify_error("Analyzer error: EXCEPT not supported")
    assert real_except == ("SetOp", "EXCEPT")


def test_subprocess_hard_timeout_covers_greylight_budget() -> None:
    """Parent hard wall must not under-cut Slow/TIMEOUT classification."""
    runner = _runner_mod()
    hard = runner.subprocess_hard_timeout_s(120.0, 300.0, 3)
    # per side: 3*120 + 300 = 660; both sides + setup 90 + grace 30 → 1440
    assert hard == 1440.0
    # Must exceed the old broken clamp (570) and a single-side Slow path (660).
    assert hard > 570.0
    assert hard >= (120.0 * 3 + 300.0) * 2 + 90.0


def test_unknown_status_is_not_exit_zero() -> None:
    """Corrupt/unknown status must not look like a clean scoreboard (exit 0)."""
    runner = _runner_mod()
    board = runner.Scoreboard(scale_factor=1.0, data_dir="", environment={})
    board.queries = [
        runner.QueryResult(
            query_nr=1,
            status="BANANA",  # type: ignore[arg-type]
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
        )
    ]
    assert runner.exit_code_for_board(board) == 4


def test_query_result_from_dict_rejects_unknown_status() -> None:
    runner = _runner_mod()
    restored = runner.query_result_from_dict(
        {
            "query_nr": 7,
            "status": "BANANA",
            "repark_wall_s": None,
            "duckdb_wall_s": None,
            "ratio": None,
            "repark_rows": None,
            "duckdb_rows": None,
        }
    )
    assert restored.status == "ERROR"
    assert restored.error_class == "InvalidStatus"


def test_query_result_from_dict_rejects_non_numeric_walls() -> None:
    """Corrupt walls must not reach report format and crash the scoreboard."""
    runner = _runner_mod()
    restored = runner.query_result_from_dict(
        {
            "query_nr": 1,
            "status": "OK",
            "repark_wall_s": "notafloat",
            "duckdb_wall_s": 0.1,
            "ratio": None,
            "repark_rows": 1,
            "duckdb_rows": 1,
        }
    )
    assert restored.status == "ERROR"
    assert restored.error_class == "InvalidPayload"
    board = runner.Scoreboard(scale_factor=1.0, data_dir="", environment={})
    board.queries = [restored]
    md = runner.render_markdown_report(board)
    assert "ERROR" in md


def test_status_ledger_rejects_unknown_status() -> None:
    import pytest

    runner = _runner_mod()
    board = runner.Scoreboard(scale_factor=1.0, data_dir="", environment={})
    board.queries = [
        runner.QueryResult(
            query_nr=1,
            status="BANANA",  # type: ignore[arg-type]
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
        )
    ]
    with pytest.raises(ValueError, match="unknown status"):
        runner.status_ledger(board)


def test_gap_census_ranks_by_count() -> None:
    runner = _runner_mod()
    board = runner.Scoreboard(scale_factor=1.0, data_dir="", environment={})
    board.queries = [
        runner.QueryResult(
            query_nr=1,
            status="ERROR",
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
            error_class="Rollup",
            missing_feature_hint="ROLLUP",
        ),
        runner.QueryResult(
            query_nr=2,
            status="ERROR",
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
            error_class="Rollup",
            missing_feature_hint="ROLLUP",
        ),
        runner.QueryResult(
            query_nr=3,
            status="OK",
            repark_wall_s=0.1,
            duckdb_wall_s=0.05,
            ratio=2.0,
            repark_rows=1,
            duckdb_rows=1,
        ),
        runner.QueryResult(
            query_nr=4,
            status="TIMEOUT",
            repark_wall_s=None,
            duckdb_wall_s=0.1,
            ratio=None,
            repark_rows=None,
            duckdb_rows=1,
            error_class="Slow",
        ),
    ]
    census = runner.gap_census(board)
    assert census[0][0] == "ROLLUP"
    assert census[0][1] == 2
    assert set(census[0][2]) == {1, 2}


def test_render_report_includes_timeout_split() -> None:
    runner = _runner_mod()
    board = runner.Scoreboard(
        scale_factor=0.01,
        data_dir="/tmp/x",
        environment={"machine": "test", "storage": "parquet-not-Iceberg (D1)"},
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="TIMEOUT",
                repark_wall_s=200.0,
                duckdb_wall_s=1.0,
                ratio=200.0,
                repark_rows=1,
                duckdb_rows=1,
                error_class="Slow",
                error_message="exceeded 120s; completed on retry",
                timeout_first_s=120.0,
                timeout_retry_s=200.0,
                ordered_compare=True,
            )
        ],
    )
    md = runner.render_markdown_report(board)
    assert "Slow=" in md
    assert "t120=" in md
    assert "t300_wall=" in md  # Slow → measured wall, not budget
    assert "ordered" in md.lower() or "| Y |" in md


def test_render_report_hung_timeout_tags_budget() -> None:
    runner = _runner_mod()
    board = runner.Scoreboard(
        scale_factor=0.01,
        data_dir="/tmp/x",
        environment={"machine": "test"},
        queries=[
            runner.QueryResult(
                query_nr=2,
                status="TIMEOUT",
                repark_wall_s=None,
                duckdb_wall_s=1.0,
                ratio=None,
                repark_rows=None,
                duckdb_rows=1,
                error_class="Timeout",
                error_message="hung",
                timeout_first_s=120.0,
                timeout_retry_s=300.0,
            )
        ],
    )
    md = runner.render_markdown_report(board)
    assert "t300_budget=" in md


def test_status_ledger_expect_query_count() -> None:
    import pytest

    runner = _runner_mod()
    board = runner.Scoreboard(scale_factor=1.0, data_dir="", environment={})
    board.queries = [
        runner.QueryResult(
            query_nr=1,
            status="OK",
            repark_wall_s=0.1,
            duckdb_wall_s=0.1,
            ratio=1.0,
            repark_rows=1,
            duckdb_rows=1,
        )
    ]
    with pytest.raises(ValueError, match="expected 99"):
        runner.status_ledger(board, expect_query_count=99)


def test_query_result_json_round_trip() -> None:
    runner = _runner_mod()
    original = runner.QueryResult(
        query_nr=42,
        status="TIMEOUT",
        repark_wall_s=250.0,
        duckdb_wall_s=1.2,
        ratio=200.0,
        repark_rows=3,
        duckdb_rows=3,
        error_class="Slow",
        error_message="slow",
        timeout_first_s=120.0,
        timeout_retry_s=250.0,
        ordered_compare=True,
    )
    payload = runner.query_result_to_dict(original)
    restored = runner.query_result_from_dict(payload)
    assert restored.query_nr == 42
    assert restored.status == "TIMEOUT"
    assert restored.error_class == "Slow"
    assert restored.timeout_first_s == 120.0
    assert restored.ordered_compare is True


def test_sf_cli_refuses_over_100() -> None:
    import importlib.util

    previous = list(sys.path)
    try:
        sys.path.insert(0, str(_TPCDS_DIR))
        spec = importlib.util.spec_from_file_location("run_tpcds_cli", _TPCDS_DIR / "run_tpcds.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.main(["--sf", "101"]) == 2
        assert module.main(["--sf", "nan"]) == 2
        assert module.main(["--sf", "0.01", "--queries", "nope"]) == 2
        assert module.main(["--sf", "0.01", "--queries", "0"]) == 2
        assert module.main(["--sf", "0.01", "--queries", ","]) == 2
    finally:
        sys.path[:] = previous


def test_run_scoreboard_refuses_empty_query_filter() -> None:
    import pytest

    runner = _runner_mod()
    with pytest.raises(ValueError, match="empty"):
        runner.run_scoreboard(scale_factor=0.01, query_filter=set(), min_free_disk_gib=0.0)


def test_timed_call_raises_on_sleep_over_budget() -> None:
    import time
    from concurrent.futures import TimeoutError as FuturesTimeout

    import pytest

    runner = _runner_mod()

    def _slow() -> list[tuple[object, ...]]:
        time.sleep(2.0)
        return [(1,)]

    with pytest.raises(FuturesTimeout):
        runner._timed_call(_slow, timeout_s=0.2)


def test_timed_call_keeps_result_when_fast() -> None:
    runner = _runner_mod()
    elapsed, rows = runner._timed_call(lambda: [(42,)], timeout_s=2.0)
    assert rows == [(42,)]
    assert elapsed < 2.0


def test_mid_repeat_exception_still_compares_prior_repark_rows() -> None:
    runner = _runner_mod()
    queries = _queries_mod()

    query = queries.TpcdsQuery(
        query_nr=99,
        original_sql="SELECT 1",
        sql_for_repark="SELECT 1",
    )
    sequence: list[object] = [
        (0.01, [(1,)]),
        (0.01, [(1,)]),
        (0.01, [(2,)]),
        RuntimeError("flake"),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):  # type: ignore[no-untyped-def]
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, Exception):
            raise item
        return item

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=2,
            timeout_s=1.0,
            timeout_retry_s=3.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]

    assert result.status == "WRONG-RESULT", result


def test_timeout_then_retry_success_is_slow() -> None:
    """Greylight: 120s TIMEOUT then 300s success → status TIMEOUT class Slow."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    runner = _runner_mod()
    queries = _queries_mod()

    query = queries.TpcdsQuery(
        query_nr=50,
        original_sql="SELECT 1",
        sql_for_repark="SELECT 1",
    )
    # repeats=1: duck ok, repark timeout, repark retry ok
    sequence: list[object] = [
        (0.01, [(1,)]),
        FuturesTimeout(),
        (200.0, [(1,)]),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):  # type: ignore[no-untyped-def]
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, BaseException):
            raise item
        return item

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=1,
            timeout_s=120.0,
            timeout_retry_s=300.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]

    assert result.status == "TIMEOUT", result
    assert result.error_class == "Slow"
    assert result.timeout_first_s == 120.0
    assert result.timeout_retry_s == 200.0
    assert result.repark_rows == 1


def test_timeout_then_retry_timeout_is_hung() -> None:
    from concurrent.futures import TimeoutError as FuturesTimeout

    runner = _runner_mod()
    queries = _queries_mod()

    query = queries.TpcdsQuery(
        query_nr=51,
        original_sql="SELECT 1",
        sql_for_repark="SELECT 1",
    )
    sequence: list[object] = [
        (0.01, [(1,)]),
        FuturesTimeout(),
        FuturesTimeout(),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):  # type: ignore[no-untyped-def]
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, BaseException):
            raise item
        return item

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=1,
            timeout_s=120.0,
            timeout_retry_s=300.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]

    assert result.status == "TIMEOUT", result
    assert result.error_class == "Timeout"
    assert "hung" in (result.error_message or "").lower()
