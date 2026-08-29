"""Unit pins for the TPC-H compare kernel (octo C3-L-003 / C1-Q-001 / C1-Q-005).

These are mutation-proof: off-by-one large integers must fail; near-equal floats pass.
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path

_TPCH_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench" / "tpch"


def _compare_mod() -> object:
    package_name = "repark_tpch_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_TPCH_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.compare")


def test_integer_off_by_one_at_million_is_wrong_result() -> None:
    """C1-Q-001: relative 1e-6 must NOT mask int off-by-one at |k| >= 1e6."""
    compare = _compare_mod()
    result = compare.compare_result_sets([(6_000_000,)], [(6_000_001,)])
    assert not result.equal, result.message


def test_float_integral_off_by_one_at_million_is_wrong_result() -> None:
    """C2-L-001: integral-valued floats must also be exact at large magnitude."""
    compare = _compare_mod()
    result = compare.compare_result_sets([(6_000_000.0,)], [(6_000_001.0,)])
    assert not result.equal, result.message


def test_decimal_off_by_one_at_million_is_wrong_result() -> None:
    """C2-L-001: integral Decimal must not float-tol into false OK."""
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
    """C2-L-002: int vs non-integral float promotes to float rules."""
    compare = _compare_mod()
    result = compare.compare_result_sets([(5,)], [(5.0000001,)])
    assert result.equal, result.message


def test_float_relative_tolerance_ok() -> None:
    compare = _compare_mod()
    # 1e-7 relative delta on ~1.0 is within 1e-6
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


def test_sf_cli_refuses_over_100() -> None:
    """C1-SEC-002 / C2-SEC-003: CLI hard-caps SF at 100 and refuses NaN."""
    import importlib.util

    previous = list(sys.path)
    try:
        sys.path.insert(0, str(_TPCH_DIR))
        spec = importlib.util.spec_from_file_location("run_tpch_cli", _TPCH_DIR / "run_tpch.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.main(["--sf", "101"]) == 2
        assert module.main(["--sf", "nan"]) == 2
    finally:
        sys.path[:] = previous


def test_timed_call_raises_on_sleep_over_budget() -> None:
    """C2-Q-002: SIGALRM path must raise FuturesTimeout for a slow worker."""
    import time
    from concurrent.futures import TimeoutError as FuturesTimeout

    import pytest

    _compare_mod()  # register package
    runner = importlib.import_module("repark_tpch_bench.runner")

    def _slow() -> list[tuple[object, ...]]:
        time.sleep(2.0)
        return [(1,)]

    with pytest.raises(FuturesTimeout):
        runner._timed_call(_slow, timeout_s=0.2)


def test_timed_call_keeps_result_when_fast() -> None:
    _compare_mod()
    runner_mod = importlib.import_module("repark_tpch_bench.runner")
    elapsed, rows = runner_mod._timed_call(lambda: [(42,)], timeout_s=2.0)
    assert rows == [(42,)]
    assert elapsed < 2.0


def test_datagen_refuses_nan_scale_factor() -> None:
    import pytest

    _compare_mod()
    datagen = importlib.import_module("repark_tpch_bench.datagen")
    with pytest.raises(ValueError, match="scale_factor"):
        datagen.ensure_parquet_sf(float("nan"))


def test_mid_repeat_exception_still_compares_prior_repark_rows() -> None:
    """C4-L-001: later Exception must not discard successful repark rows."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")

    query = queries.TpchQuery(
        query_nr=99,
        original_sql="SELECT 1",
        sql_for_repark="SELECT 1",
    )
    # repeats=2: duck, duck, repark wrong, repark flake
    sequence: list[object] = [
        (0.01, [(1,)]),
        (0.01, [(1,)]),
        (0.01, [(2,)]),
        RuntimeError("flake"),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, Exception):
            raise item
        return item  # type: ignore[return-value]

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=2,
            timeout_s=1.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]

    assert result.status == "WRONG-RESULT", result
    assert result.repark_rows == 1
    assert result.duckdb_rows == 1


def test_mid_repeat_duck_timeout_keeps_oracle_and_compares() -> None:
    """C4-L-002: DuckDB timeout after success still compares repark."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")

    query = queries.TpchQuery(
        query_nr=98,
        original_sql="SELECT 1",
        sql_for_repark="SELECT 1",
    )
    # repeats=2: duck ok, duck timeout, repark ok, repark ok
    sequence: list[object] = [
        (0.01, [(7,)]),
        FuturesTimeout(),
        (0.01, [(7,)]),
        (0.01, [(7,)]),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, BaseException):
            raise item
        return item  # type: ignore[return-value]

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=2,
            timeout_s=1.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]

    assert result.status == "OK", result


def test_mid_repeat_repark_timeout_still_compares_prior_rows() -> None:
    """C5-L-001: repark FuturesTimeout after success must compare, not TIMEOUT."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")

    query = queries.TpchQuery(
        query_nr=97,
        original_sql="SELECT 1",
        sql_for_repark="SELECT 1",
    )
    # repeats=2: duck, duck, repark wrong, repark timeout
    sequence: list[object] = [
        (0.01, [(1,)]),
        (0.01, [(1,)]),
        (0.01, [(2,)]),
        FuturesTimeout(),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, BaseException):
            raise item
        return item  # type: ignore[return-value]

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=2,
            timeout_s=1.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]

    assert result.status == "WRONG-RESULT", result


def test_bool_not_equal_to_int_one() -> None:
    """E1-L-004: True must not equal 1 under the TPC-H compare kernel."""
    compare = _compare_mod()
    result = compare.compare_result_sets([(True,)], [(1,)])
    assert not result.equal, result.message


def test_first_timeout_drains_remaining_repeats() -> None:
    """E1-L-001: first-side timeout continues remaining attempts."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")
    query = queries.TpchQuery(query_nr=96, original_sql="SELECT 1", sql_for_repark="SELECT 1")
    # duck: timeout then ok x2; repark: ok x3
    sequence: list[object] = [
        FuturesTimeout(),
        (0.01, [(1,)]),
        (0.01, [(1,)]),
        (0.01, [(1,)]),
        (0.01, [(1,)]),
        (0.01, [(1,)]),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, BaseException):
            raise item
        return item  # type: ignore[return-value]

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=3,
            timeout_s=1.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]
    assert result.status == "OK", result


def test_earlier_wrong_payload_not_overwritten_by_later_ok() -> None:
    """E1-L-002: any successful wrong payload forces WRONG-RESULT."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")
    query = queries.TpchQuery(query_nr=95, original_sql="SELECT 1", sql_for_repark="SELECT 1")
    # duck x2, repark wrong, repark ok
    sequence: list[object] = [
        (0.01, [(1,)]),
        (0.01, [(1,)]),
        (0.01, [(2,)]),
        (0.01, [(1,)]),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        return item  # type: ignore[return-value]

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=2,
            timeout_s=1.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]
    assert result.status == "WRONG-RESULT", result


def test_cli_usage_exit_is_two_not_wrong_result() -> None:
    """Usage errors stay exit 2; WRONG-RESULT uses 3 (E1-L-005)."""
    import importlib.util

    previous = list(sys.path)
    try:
        sys.path.insert(0, str(_TPCH_DIR))
        spec = importlib.util.spec_from_file_location("run_tpch_cli2", _TPCH_DIR / "run_tpch.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.main(["--sf", "101"]) == 2
    finally:
        sys.path[:] = previous


def test_timeout_then_exception_is_error_not_timeout() -> None:
    """E2-L-001: timeout drain then hard error must report ERROR."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")
    query = queries.TpchQuery(query_nr=94, original_sql="SELECT 1", sql_for_repark="SELECT 1")
    sequence: list[object] = [
        FuturesTimeout(),
        RuntimeError("analyzer boom"),
    ]
    index = {"i": 0}

    def _timed(function, *, timeout_s):
        del function, timeout_s
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, BaseException):
            raise item
        return item  # type: ignore[return-value]

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=2,
            timeout_s=1.0,
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]
    assert result.status == "ERROR", result
    assert "analyzer boom" in (result.error_message or "")


def test_exit_code_priority_wrong_error_timeout() -> None:
    """E2-L-002: WRONG=3 beats ERROR=4 beats TIMEOUT=5."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")

    def _q(nr: int, status: str) -> object:
        return runner.QueryResult(
            query_nr=nr,
            status=status,
            repark_wall_s=None,
            duckdb_wall_s=None,
            ratio=None,
            repark_rows=None,
            duckdb_rows=None,
        )

    board = runner.Scoreboard(
        scale_factor=0.01,
        data_dir="/tmp",
        environment={},
        queries=[_q(1, "TIMEOUT"), _q(2, "ERROR"), _q(3, "WRONG-RESULT")],
    )
    assert runner.exit_code_for_board(board) == 3
    board.queries = [_q(1, "TIMEOUT"), _q(2, "ERROR")]
    assert runner.exit_code_for_board(board) == 4
    board.queries = [_q(1, "TIMEOUT")]
    assert runner.exit_code_for_board(board) == 5
    board.queries = [_q(1, "OK")]
    assert runner.exit_code_for_board(board) == 0


def test_cache_refuses_symlink_and_zero_size(tmp_path: object) -> None:
    """E2-SEC-001: _cache_is_usable rejects symlink and empty files."""
    from pathlib import Path

    _compare_mod()
    datagen = importlib.import_module("repark_tpch_bench.datagen")
    root = Path(str(tmp_path)) / "sf0.01"
    root.mkdir()
    for name in datagen.TABLES:
        (root / f"{name}.parquet").write_bytes(b"x")
    assert datagen._cache_is_usable(root)
    # zero-size
    (root / "customer.parquet").write_bytes(b"")
    assert not datagen._cache_is_usable(root)
    (root / "customer.parquet").write_bytes(b"x")
    # symlink
    target = root / "customer.parquet"
    target.unlink()
    target.symlink_to("/etc/hosts")
    assert not datagen._cache_is_usable(root)


def test_smoke_fixture_uses_private_data_root() -> None:
    """E2-SEC-001: smoke pin path is not sticky /tmp/tpch-data."""
    text = (Path(__file__).resolve().parent / "test_tpch_smoke.py").read_text(encoding="utf-8")
    assert "tmp_path_factory.mktemp" in text
    assert "data_root=root" in text
    # Bare ensure_parquet_sf(0.01) without data_root must not appear.
    bare = "ensure_parquet_sf(0.01)"
    assert bare not in text.replace("ensure_parquet_sf(0.01, data_root=root)", "")


def test_default_data_root_is_not_sticky_tmp() -> None:
    """E7-SEC-001: default cache is under user cache, not /tmp/tpch-data."""
    _compare_mod()
    datagen = importlib.import_module("repark_tpch_bench.datagen")
    root = datagen.default_data_root()
    assert root.name == "repark-tpch"
    assert not str(root).startswith("/tmp/tpch-data")


def test_refuses_symlink_cache_root(tmp_path: object) -> None:
    """E7-SEC-001: directory symlink as data_root is refused."""
    from pathlib import Path

    import pytest

    _compare_mod()
    datagen = importlib.import_module("repark_tpch_bench.datagen")
    base = Path(str(tmp_path))
    real = base / "real"
    real.mkdir()
    link = base / "link"
    link.symlink_to(real)
    with pytest.raises(ValueError, match="symlink"):
        datagen.ensure_parquet_sf(0.01, data_root=link)


# R-TPCH-V3 — disk gate, DIED exit, isolation defaults, report labels


def test_sf10_disk_gate_below_threshold_skips(tmp_path: object, monkeypatch: object) -> None:
    """Free disk < 30 GiB → not ok_to_run."""
    from pathlib import Path

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")

    def _fake_free(_path: Path) -> float:
        return 12.5

    monkeypatch.setattr(runner, "free_disk_gib", _fake_free)  # type: ignore[attr-defined]
    ok, free = runner.sf10_disk_gate(Path(str(tmp_path)), min_free_gib=30.0)
    assert not ok
    assert free == 12.5


def test_sf10_disk_gate_above_threshold_ok(tmp_path: object, monkeypatch: object) -> None:
    from pathlib import Path

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    monkeypatch.setattr(runner, "free_disk_gib", lambda _p: 64.0)  # type: ignore[attr-defined]
    ok, free = runner.sf10_disk_gate(Path(str(tmp_path)), min_free_gib=30.0)
    assert ok
    assert free == 64.0


def test_run_scoreboard_sf10_skips_when_disk_low(tmp_path: object, monkeypatch: object) -> None:
    """SF10 scoreboard short-circuits with FINDING, no queries."""
    from pathlib import Path

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    monkeypatch.setattr(runner, "free_disk_gib", lambda _p: 5.0)  # type: ignore[attr-defined]
    board = runner.run_scoreboard(scale_factor=10.0, data_root=Path(str(tmp_path)), repeats=1)
    assert board.skipped
    assert board.queries == []
    assert any("SKIPPED" in finding for finding in board.findings)
    assert runner.exit_code_for_board(board) == 0


def test_exit_code_died_is_six() -> None:
    """DIED maps to exit 6; outranks TIMEOUT; WRONG still beats DIED (C1-Q-001)."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")

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

    board = runner.Scoreboard(
        scale_factor=10.0,
        data_dir="/tmp",
        environment={},
        queries=[_q("DIED")],
    )
    assert runner.exit_code_for_board(board) == 6
    board.queries = [_q("DIED"), _q("TIMEOUT")]
    assert runner.exit_code_for_board(board) == 6
    board.queries = [_q("DIED"), _q("WRONG-RESULT")]
    assert runner.exit_code_for_board(board) == 3


def test_query_result_roundtrip_dict() -> None:
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    original = runner.QueryResult(
        query_nr=7,
        status="OK",
        repark_wall_s=1.25,
        duckdb_wall_s=0.5,
        ratio=2.5,
        repark_rows=4,
        duckdb_rows=4,
        rss_peak_kb=12345,
    )
    restored = runner.query_result_from_dict(runner.query_result_to_dict(original))
    assert restored.query_nr == 7
    assert restored.status == "OK"
    assert restored.rss_peak_kb == 12345
    assert restored.repark_wall_s == 1.25


def test_iceberg_report_uses_iceberg_wall_column() -> None:
    """Iceberg leg MD matrix labels the wall column iceberg_wall."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/tmp",
        environment={
            "storage": "Iceberg memory-catalog (local warehouse; V3 leg)",
            "storage_kind": "iceberg",
            "isolation": "inprocess",
        },
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="OK",
                repark_wall_s=0.5,
                duckdb_wall_s=0.2,
                ratio=2.5,
                repark_rows=1,
                duckdb_rows=1,
                rss_peak_kb=100,
            )
        ],
    )
    md = runner.render_markdown_report(board, title="Iceberg leg")
    assert "iceberg_wall" in md
    assert "| 1 | OK | 0.500 |" in md


def test_parquet_report_not_iceberg_wall_despite_not_iceberg_label() -> None:
    """'parquet-not-Iceberg' must not flip the wall column to iceberg_wall."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    board = runner.Scoreboard(
        scale_factor=10.0,
        data_dir="/tmp",
        environment={
            "storage": "parquet-not-Iceberg (fast seed path)",
            "storage_kind": "parquet",
            "isolation": "subprocess",
        },
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="OK",
                repark_wall_s=0.5,
                duckdb_wall_s=0.2,
                ratio=2.5,
                repark_rows=1,
                duckdb_rows=1,
            )
        ],
    )
    md = runner.render_markdown_report(board)
    assert "repark_s" in md
    assert "iceberg_wall" not in md


def test_subprocess_signal_maps_to_died(monkeypatch: object) -> None:
    """Negative worker returncode → DIED status."""
    from pathlib import Path

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")

    class _FakeCompleted:
        returncode = -9  # SIGKILL
        stderr = ""
        stdout = ""

    def _fake_run(*_a: object, **_k: object) -> _FakeCompleted:
        return _FakeCompleted()

    # Workers use kill-group helper (C1-L-001), not bare subprocess.run.
    monkeypatch.setattr(runner, "_subprocess_run_kill_group", _fake_run)
    query = queries.TpchQuery(query_nr=3, original_sql="SELECT 1", sql_for_repark="SELECT 1")
    result = runner._run_one_query_subprocess(
        data_dir=Path("/tmp"),
        query=query,
        repeats=1,
        timeout_s=1.0,
        storage="parquet",
        warehouse=None,
    )
    assert result.status == "DIED"
    assert result.error_class == "Signal"


def test_cli_still_refuses_sf_over_100() -> None:
    """CLI refuses SF>100 (usage exit 2)."""
    import importlib.util

    previous = list(sys.path)
    try:
        sys.path.insert(0, str(_TPCH_DIR))
        spec = importlib.util.spec_from_file_location("run_tpch_cli_v3", _TPCH_DIR / "run_tpch.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.main(["--sf", "101"]) == 2
        assert module.main(["--sf", "nan"]) == 2
    finally:
        sys.path[:] = previous


# R-SAIL-BENCH: timeout retry + three-way merge (no pysail required)


def test_timeout_retry_slow_class_when_retry_succeeds() -> None:
    """First-pass TIMEOUT then 300s success → status TIMEOUT error_class Slow."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")
    query = queries.TpchQuery(query_nr=90, original_sql="SELECT 1", sql_for_repark="SELECT 1")
    # duck ok; subject: timeout on first pass (repeats=1), then retry success
    sequence: list[object] = [
        (0.01, [(1,)]),
        FuturesTimeout(),
        (2.5, [(1,)]),
    ]
    index = {"i": 0}
    seen_timeouts: list[float] = []

    def _timed(function, *, timeout_s):  # type: ignore[no-untyped-def]
        del function
        seen_timeouts.append(float(timeout_s))
        item = sequence[index["i"]]
        index["i"] += 1
        if isinstance(item, BaseException):
            raise item
        return item  # type: ignore[return-value]

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=1,
            timeout_s=1.0,
            timeout_retry_s=300.0,
            subject_label="repark",
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]

    assert result.status == "TIMEOUT", result
    assert result.error_class == "Slow"
    assert result.repark_wall_s == 2.5
    assert 300.0 in seen_timeouts


def test_timeout_retry_hung_when_retry_also_times_out() -> None:
    """First-pass + 300s retry both TIMEOUT → hung Timeout (not Slow)."""
    from concurrent.futures import TimeoutError as FuturesTimeout

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")
    query = queries.TpchQuery(query_nr=91, original_sql="SELECT 1", sql_for_repark="SELECT 1")
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
        return item  # type: ignore[return-value]

    original = runner._timed_call
    runner._timed_call = _timed  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=1,
            timeout_s=1.0,
            timeout_retry_s=300.0,
            subject_label="sail",
        )
    finally:
        runner._timed_call = original  # type: ignore[method-assign]

    assert result.status == "TIMEOUT", result
    assert result.error_class == "Timeout"
    assert "hung" in (result.error_message or "")
    assert result.repark_wall_s is None


def test_merge_three_way_fills_sail_columns() -> None:
    """merge_three_way combines repark + sail boards into three wall columns."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")

    repark_board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache/sf1",
        environment={"subject_engine": "repark", "machine": "host"},
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="OK",
                repark_wall_s=0.4,
                duckdb_wall_s=0.1,
                ratio=4.0,
                repark_rows=10,
                duckdb_rows=10,
            ),
            runner.QueryResult(
                query_nr=2,
                status="ERROR",
                repark_wall_s=None,
                duckdb_wall_s=0.2,
                ratio=None,
                repark_rows=None,
                duckdb_rows=5,
                error_class="Syntax",
            ),
        ],
    )
    sail_board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache/sf1",
        environment={
            "subject_engine": "sail",
            "pysail_version": "0.6.6",
            "pyspark_client_version": "4.1.1",
        },
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="OK",
                repark_wall_s=0.8,
                duckdb_wall_s=0.1,
                ratio=8.0,
                repark_rows=10,
                duckdb_rows=10,
            ),
            runner.QueryResult(
                query_nr=2,
                status="WRONG-RESULT",
                repark_wall_s=0.3,
                duckdb_wall_s=0.2,
                ratio=1.5,
                repark_rows=4,
                duckdb_rows=5,
                error_class="WrongResult",
            ),
        ],
    )
    merged = runner.merge_three_way(repark_board, sail_board)
    assert len(merged.queries) == 2
    q1 = merged.queries[0]
    assert q1.repark_status == "OK"
    assert q1.sail_status == "OK"
    assert q1.repark_wall_s == 0.4
    assert q1.sail_wall_s == 0.8
    assert q1.duckdb_wall_s == 0.1
    assert q1.status == "OK"
    q2 = merged.queries[1]
    assert q2.repark_status == "ERROR"
    assert q2.sail_status == "WRONG-RESULT"
    assert q2.status == "WRONG-RESULT"
    assert merged.environment["subject_engine"] == "both"
    assert "pysail_version" in merged.environment
    md = runner.render_markdown_report(merged, title="three-way")
    assert "sail_s" in md
    assert "repark_s" in md
    assert "duckdb_s" in md
    assert "measurement prior-art only" in md
    assert "gRPC loopback" in md


def test_worse_status_ranking() -> None:
    """Overall three-way status ranks WRONG-RESULT worst."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    assert runner.worse_status("OK", "TIMEOUT") == "TIMEOUT"
    assert runner.worse_status("TIMEOUT", "ERROR") == "ERROR"
    assert runner.worse_status("ERROR", "DIED") == "DIED"
    assert runner.worse_status("DIED", "WRONG-RESULT") == "WRONG-RESULT"
    assert runner.worse_status("OK", "OK") == "OK"
    # C4-Q-002: unknown ranks as ERROR-equivalent, never OK.
    assert runner.worse_status("OK", "BANANA") == "ERROR"  # type: ignore[arg-type]
    assert runner.worse_status("BANANA", "TIMEOUT") == "ERROR"  # type: ignore[arg-type]


def test_query_result_from_dict_coerces_invalid_status() -> None:
    """C4-Q-001: hostile Sail JSON status must not green-exit the scoreboard."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    result = runner.query_result_from_dict(
        {
            "query_nr": 1,
            "status": "BANANA",
            "repark_wall_s": None,
            "duckdb_wall_s": None,
            "ratio": None,
            "repark_rows": None,
            "duckdb_rows": None,
        }
    )
    assert result.status == "ERROR"
    assert result.error_class == "InvalidStatus"
    board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="",
        environment={},
        queries=[result],
    )
    assert runner.exit_code_for_board(board) == 4


def test_cli_engine_and_timeout_retry_flags() -> None:
    """CLI accepts --engine / --timeout-retry; rejects bad timeout-retry."""
    import importlib.util

    previous = list(sys.path)
    try:
        sys.path.insert(0, str(_TPCH_DIR))
        spec = importlib.util.spec_from_file_location("run_tpch_cli_b1", _TPCH_DIR / "run_tpch.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.main(["--timeout-retry", "0"]) == 2
        assert module.main(["--timeout-retry", "99999"]) == 2
        # --engine sail is accepted by argparse; SF usage refusal still applies with the flag.
        assert module.main(["--engine", "sail", "--sf", "101"]) == 2
        assert module.main(["--engine", "both", "--sf", "0"]) == 2
        # C3-Q-001: bad --queries must be usage exit 2, not traceback.
        assert module.main(["--queries", "abc", "--sf", "0.01"]) == 2
        assert module.main(["--queries", ",,,", "--sf", "0.01"]) == 2
    finally:
        sys.path[:] = previous


def test_sail_engine_module_importable_without_pysail() -> None:
    """sail_engine loads; require_sail_imports fails loudly without pysail."""
    _compare_mod()
    sail_engine = importlib.import_module("repark_tpch_bench.sail_engine")
    try:
        sail_engine.require_sail_imports()
    except sail_engine.SailUnavailableError as exc:
        assert "pysail" in str(exc).lower() or "pyspark" in str(exc).lower()
    else:
        # Sail may be present on the measurement host; that is fine.
        pass
    versions = sail_engine.sail_package_versions()
    assert "pysail" in versions
    assert "pyspark" in versions


def test_compare_subject_label_sail_not_repark() -> None:
    """C1-H-001: Sail WRONG-RESULT messages must not mislabel the subject as repark."""
    compare = _compare_mod()
    result = compare.compare_result_sets(
        [(1.0,)],
        [(2.0,)],
        subject_label="sail",
    )
    assert not result.equal
    assert "sail=" in result.message
    assert "repark=" not in result.message


def test_sail_unavailable_board_is_skipped_exit_zero() -> None:
    """C1-Q-001: empty sail-unavailable board is skipped FINDING (exit 0), not success green."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache",
        environment={"subject_engine": "sail", "sail_status": "unavailable"},
        findings=["Sail unavailable: no pysail"],
        skipped=True,
    )
    assert runner.exit_code_for_board(board) == 0
    md = runner.render_markdown_report(board, title="sail-skip")
    assert "Skipped" in md
    assert "all queries OK" not in md


def test_run_sail_scoreboard_only_fallthrough_on_unavailable(monkeypatch: object) -> None:
    """C1-Q-002: scoreboard exceptions must not be re-run via sail_python subprocess."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    sail_engine = importlib.import_module("repark_tpch_bench.sail_engine")

    monkeypatch.setattr(sail_engine, "require_sail_imports", lambda: (object(), object()))

    def _boom(**_kwargs: object) -> object:
        raise RuntimeError("scoreboard mid-run failure")

    monkeypatch.setattr(runner, "run_scoreboard", _boom)
    called = {"subprocess": False}

    def _no_sub(**_kwargs: object) -> object:
        called["subprocess"] = True
        msg = "must not reach sail_python path"
        raise AssertionError(msg)

    monkeypatch.setattr(runner, "_run_sail_scoreboard_subprocess", _no_sub)
    try:
        runner._run_sail_scoreboard(
            scale_factor=0.01,
            data_root=None,
            repeats=1,
            timeout_s=1.0,
            timeout_retry_s=2.0,
            query_filter={1},
            isolation=None,
            min_free_disk_gib=30.0,
            sail_python=None,
        )
        raise AssertionError("expected RuntimeError from in-process scoreboard")
    except RuntimeError as exc:
        assert "mid-run failure" in str(exc)
    assert called["subprocess"] is False


def test_merge_three_way_missing_sail_keeps_repark_overall() -> None:
    """C1-T-001 / C2-H-002: skipped empty Sail board must not invent per-query ERROR."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    repark_board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache",
        environment={"subject_engine": "repark"},
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="OK",
                repark_wall_s=0.1,
                duckdb_wall_s=0.05,
                ratio=2.0,
                repark_rows=1,
                duckdb_rows=1,
            )
        ],
    )
    sail_board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="",
        environment={"subject_engine": "sail", "sail_status": "skipped"},
        queries=[],
        skipped=True,
        findings=["Sail leg SKIPPED"],
    )
    merged = runner.merge_three_way(repark_board, sail_board)
    assert len(merged.queries) == 1
    assert merged.queries[0].status == "OK"
    assert merged.queries[0].sail_status is None
    assert merged.queries[0].sail_error_class == "SailBoardSkipped"
    # Non-skipped empty sail board still ERROR.
    partial = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache",
        environment={"subject_engine": "sail"},
        queries=[],
        skipped=False,
    )
    partial_merged = runner.merge_three_way(repark_board, partial)
    assert partial_merged.queries[0].sail_status == "ERROR"
    assert partial_merged.queries[0].sail_error_class == "MissingSailRow"


def test_merge_three_way_falls_back_to_sail_timeout_metadata() -> None:
    """C1-H-002: three-way keeps Sail Slow/hung timeout fields when repark has none."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    repark_board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache",
        environment={"subject_engine": "repark"},
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="OK",
                repark_wall_s=0.1,
                duckdb_wall_s=0.05,
                ratio=2.0,
                repark_rows=1,
                duckdb_rows=1,
            )
        ],
    )
    sail_board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache",
        environment={"subject_engine": "sail"},
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="TIMEOUT",
                repark_wall_s=2.5,
                duckdb_wall_s=0.05,
                ratio=50.0,
                repark_rows=1,
                duckdb_rows=1,
                error_class="Slow",
                timeout_first_s=1.0,
                timeout_retry_s=2.5,
            )
        ],
    )
    merged = runner.merge_three_way(repark_board, sail_board)
    assert merged.queries[0].status == "TIMEOUT"
    assert merged.queries[0].timeout_first_s == 1.0
    assert merged.queries[0].timeout_retry_s == 2.5
    md = runner.render_markdown_report(merged, title="three-way-slow")
    assert "t120=" in md


def test_sail_subprocess_hard_wall_uses_sf10_default_timeout() -> None:
    """C1-Q-004 / C5: sail board hard wall uses SF10 300s default and query-filter size."""
    import subprocess

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    captured: dict[str, float] = {}

    def _fake_run(command: object, *, timeout_s: float) -> object:  # type: ignore[no-untyped-def]
        del command
        captured["timeout_s"] = timeout_s
        raise subprocess.TimeoutExpired(cmd=["x"], timeout=timeout_s)

    original = runner._subprocess_run_kill_group
    runner._subprocess_run_kill_group = _fake_run  # type: ignore[method-assign]
    try:
        board = runner._run_sail_scoreboard_subprocess(
            python_path=Path(sys.executable),
            scale_factor=10.0,
            data_root=None,
            repeats=1,
            timeout_s=None,
            timeout_retry_s=None,
            query_filter={1},
            isolation=None,
            min_free_disk_gib=30.0,
        )
    finally:
        runner._subprocess_run_kill_group = original  # type: ignore[method-assign]

    assert board.skipped
    assert "timeout" in board.environment.get("sail_status", "")
    # 1 query * (1*300 + 300) * 2 + 600 = 1800; floor max(600, 1800).
    assert captured["timeout_s"] == 1.0 * (300.0 + 300.0) * 2.0 + 600.0


def test_classify_error_sail_grpc() -> None:
    """C5: Spark Connect transport errors map to SailGrpc / SailConnect."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    error_class, hint = runner.classify_error("io.grpc.StatusRuntimeException: UNAVAILABLE")
    assert error_class == "SailGrpc"
    assert hint is not None
    error_class2, _hint2 = runner.classify_error("Connection refused to sc://localhost")
    assert error_class2 == "SailConnect"


def test_three_way_skipped_sail_no_grpc_cost_claim() -> None:
    """C6-H-001: skipped Sail board must not claim gRPC transport cost in disclosure."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    repark_board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache",
        environment={"subject_engine": "repark"},
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="OK",
                repark_wall_s=0.1,
                duckdb_wall_s=0.05,
                ratio=2.0,
                repark_rows=1,
                duckdb_rows=1,
            )
        ],
    )
    sail_board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="",
        environment={"subject_engine": "sail", "sail_status": "skipped"},
        queries=[],
        skipped=True,
        findings=["Sail leg SKIPPED"],
    )
    merged = runner.merge_three_way(repark_board, sail_board)
    md = runner.render_markdown_report(merged, title="skipped-sail")
    assert "pays gRPC loopback" not in md
    assert "did not run" in md
    # Successful three-way still discloses gRPC:
    sail_ok = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache",
        environment={
            "subject_engine": "sail",
            "pysail_version": "0.6.6",
            "sail_port": "1",
        },
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="OK",
                repark_wall_s=0.2,
                duckdb_wall_s=0.05,
                ratio=4.0,
                repark_rows=1,
                duckdb_rows=1,
            )
        ],
    )
    ok_md = runner.render_markdown_report(
        runner.merge_three_way(repark_board, sail_ok), title="ok-sail"
    )
    assert "pays gRPC loopback" in ok_md


def test_subprocess_run_kill_group_exists_and_uses_new_session(monkeypatch: object) -> None:
    """C1-L-001: helper starts a new session so grandchildren can be killpg'd."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    import subprocess

    seen: dict[str, object] = {}

    class _FakeProc:
        pid = 4242
        returncode = 0

        def communicate(self, timeout: float | None = None) -> tuple[str, str]:
            del timeout
            return ("ok", "")

        def kill(self) -> None:
            return None

    def _fake_popen(*_args: object, **kwargs: object) -> _FakeProc:
        seen["start_new_session"] = kwargs.get("start_new_session")
        return _FakeProc()

    monkeypatch.setattr(subprocess, "Popen", _fake_popen)
    completed = runner._subprocess_run_kill_group(["true"], timeout_s=5.0)
    assert seen["start_new_session"] is True
    assert completed.returncode == 0
    assert completed.stdout == "ok"


def test_three_way_per_engine_census_includes_died() -> None:
    """C2-H-001: per-engine census must count DIED (not only TIMEOUT/ERROR)."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    board = runner.Scoreboard(
        scale_factor=1.0,
        data_dir="/cache",
        environment={"subject_engine": "both"},
        queries=[
            runner.QueryResult(
                query_nr=1,
                status="DIED",
                repark_wall_s=0.1,
                duckdb_wall_s=0.05,
                ratio=2.0,
                repark_rows=1,
                duckdb_rows=1,
                repark_status="OK",
                sail_status="DIED",
                sail_error_class="Signal",
            )
        ],
    )
    md = runner.render_markdown_report(board, title="died-census")
    assert "DIED=1" in md
    assert "Sail: OK=0 WRONG-RESULT=0 ERROR=0 TIMEOUT=0 DIED=1" in md


def test_sail_board_malformed_json_is_skipped_finding(
    tmp_path: object, monkeypatch: object
) -> None:
    """C3-Q-002: invalid Sail board JSON must not crash engine=both merge path."""
    from pathlib import Path

    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")

    def _fake_run(command: object, *, timeout_s: float) -> object:  # type: ignore[no-untyped-def]
        del timeout_s
        command_list = list(command)  # type: ignore[arg-type]
        out_index = command_list.index("--out") + 1
        out_path = Path(command_list[out_index])
        out_path.write_text("{not-json", encoding="utf-8")

        class _Done:
            returncode = 0
            stderr = ""
            stdout = ""

        return _Done()

    monkeypatch.setattr(runner, "_subprocess_run_kill_group", _fake_run)
    board = runner._run_sail_scoreboard_subprocess(
        python_path=Path(sys.executable),
        scale_factor=1.0,
        data_root=None,
        repeats=1,
        timeout_s=1.0,
        timeout_retry_s=1.0,
        query_filter={1},
        isolation=None,
        min_free_disk_gib=30.0,
    )
    assert board.skipped
    assert board.environment.get("sail_status") == "board_error"
    assert any("JSON invalid" in finding for finding in board.findings)


def test_sail_uses_original_sql_not_repark_rewrite() -> None:
    """C2-Q-001: Sail subject path must not consume repark dialect rewrites."""
    _compare_mod()
    runner = importlib.import_module("repark_tpch_bench.runner")
    queries = importlib.import_module("repark_tpch_bench.queries")
    query = queries.TpchQuery(
        query_nr=99,
        original_sql="SELECT 1 AS original",
        sql_for_repark="SELECT 1 AS rewritten",
        rewrite_note="dialect only",
    )
    seen_sql: list[str] = []
    original_timed = runner._timed_call
    original_subject = runner._subject_collect
    original_duck = runner._duckdb_collect

    def _capture_subject(spark, sql, *, subject_label="repark"):  # type: ignore[no-untyped-def]
        del spark
        seen_sql.append(sql)
        assert subject_label == "sail"
        return [(1,)]

    def _timed(function, *, timeout_s):  # type: ignore[no-untyped-def]
        del timeout_s
        return 0.01, function()

    runner._timed_call = _timed  # type: ignore[method-assign]
    runner._subject_collect = _capture_subject  # type: ignore[method-assign]
    runner._duckdb_collect = lambda *_a, **_k: [(1,)]  # type: ignore[method-assign]
    try:
        result = runner._run_one_query(
            spark=object(),
            duckdb_conn=object(),
            query=query,
            repeats=1,
            timeout_s=1.0,
            timeout_retry_s=0.0,
            subject_label="sail",
        )
    finally:
        runner._timed_call = original_timed  # type: ignore[method-assign]
        runner._subject_collect = original_subject  # type: ignore[method-assign]
        runner._duckdb_collect = original_duck  # type: ignore[method-assign]

    assert result.status == "OK"
    assert seen_sql == ["SELECT 1 AS original"]
    assert "rewritten" not in seen_sql[0]


def test_default_engine_is_repark_and_does_not_import_sail() -> None:
    """C7: default scoreboard engine remains repark; sail_engine stays optional."""
    import inspect

    _compare_mod()
    # Fresh package load without sail_engine.
    for name in list(sys.modules):
        if name.startswith("repark_tpch_bench"):
            del sys.modules[name]
    package_name = "repark_tpch_bench"
    package = types.ModuleType(package_name)
    package.__path__ = [str(_TPCH_DIR)]  # type: ignore[attr-defined]
    sys.modules[package_name] = package
    runner = importlib.import_module(f"{package_name}.runner")
    assert inspect.signature(runner.run_scoreboard).parameters["engine"].default == "repark"
    assert f"{package_name}.sail_engine" not in sys.modules


# G10 — baseline-ratios gate (schedule job helper)


def _check_baseline_mod() -> object:
    package_name = "repark_tpch_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_TPCH_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.check_baseline_ratios")


def test_baseline_ratio_within_ceiling_is_ok() -> None:
    """G10: OK query with ratio under ceiling exits 0."""
    check = _check_baseline_mod()
    scoreboard = {
        "queries": [
            {"query_nr": 1, "status": "OK", "ratio": 2.0},
            {"query_nr": 2, "status": "OK", "ratio": 1.5},
        ]
    }
    baseline = {
        "provisional": True,
        "queries": {
            "1": {"measured_ratio": 2.0, "ceiling": 3.0},
            "2": {"measured_ratio": 1.5, "ceiling": 2.25},
        },
    }
    assert check.compare(scoreboard, baseline) == 0


def test_baseline_ratio_over_ceiling_fails() -> None:
    """G10: OK query over ceiling exits 1."""
    check = _check_baseline_mod()
    scoreboard = {"queries": [{"query_nr": 17, "status": "OK", "ratio": 25.0}]}
    baseline = {"queries": {"17": {"measured_ratio": 12.8, "ceiling": 19.2}}}
    assert check.compare(scoreboard, baseline) == 1


def test_baseline_wrong_result_fails() -> None:
    """G10: WRONG-RESULT is a gate failure even when ratio is fine."""
    check = _check_baseline_mod()
    scoreboard = {"queries": [{"query_nr": 1, "status": "WRONG-RESULT", "ratio": 1.0}]}
    baseline = {"queries": {"1": {"ceiling": 10.0}}}
    assert check.compare(scoreboard, baseline) == 1


def test_committed_baseline_ratios_file_has_22_ceilings() -> None:
    """G10: committed baseline-ratios.json covers all 22 TPC-H queries."""
    path = _TPCH_DIR / "baseline-ratios.json"
    assert path.is_file(), path
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload.get("provisional") is True
    queries = payload["queries"]
    assert len(queries) == 22
    for query_nr in range(1, 23):
        entry = queries[str(query_nr)]
        assert "ceiling" in entry
        assert float(entry["ceiling"]) > 0


def test_baseline_empty_scoreboard_fails() -> None:
    """G10 C1-L-001: empty scoreboard must not green-exit."""
    check = _check_baseline_mod()
    baseline = {"queries": {"1": {"ceiling": 10.0}}}
    assert check.compare({"queries": []}, baseline) == 1
    assert check.compare({}, baseline) == 1


def test_baseline_missing_query_from_scoreboard_fails() -> None:
    """G10 C1-L-001: partial scoreboard missing a baseline Q fails."""
    check = _check_baseline_mod()
    scoreboard = {"queries": [{"query_nr": 1, "status": "OK", "ratio": 1.0}]}
    baseline = {
        "queries": {
            "1": {"ceiling": 10.0},
            "2": {"ceiling": 10.0},
        }
    }
    assert check.compare(scoreboard, baseline) == 1


def test_baseline_zero_ok_checked_fails() -> None:
    """G10 C1-L-001: OK rows with no matching ceilings → fail (not 0-check OK)."""
    check = _check_baseline_mod()
    scoreboard = {"queries": [{"query_nr": 99, "status": "OK", "ratio": 1.0}]}
    baseline = {"queries": {"1": {"ceiling": 10.0}}}
    # Missing Q1 from board + ok_checked=0 for Q99 without ceiling.
    assert check.compare(scoreboard, baseline) == 1


def test_committed_baseline_full_22_under_ceiling_ok() -> None:
    """G10 C2-Q-002: committed baseline accepts full 22-OK under ceilings."""
    check = _check_baseline_mod()
    path = _TPCH_DIR / "baseline-ratios.json"
    baseline = json.loads(path.read_text(encoding="utf-8"))
    scoreboard = {
        "queries": [
            {
                "query_nr": query_nr,
                "status": "OK",
                "ratio": float(baseline["queries"][str(query_nr)]["ceiling"]) - 0.01,
            }
            for query_nr in range(1, 23)
        ]
    }
    assert check.compare(scoreboard, baseline) == 0


def test_committed_baseline_full_22_one_over_fails() -> None:
    """G10 C2-Q-002: one over-ceiling among full 22 fails the gate."""
    check = _check_baseline_mod()
    path = _TPCH_DIR / "baseline-ratios.json"
    baseline = json.loads(path.read_text(encoding="utf-8"))
    scoreboard = {
        "queries": [
            {
                "query_nr": query_nr,
                "status": "OK",
                "ratio": float(baseline["queries"][str(query_nr)]["ceiling"]) - 0.01,
            }
            for query_nr in range(1, 23)
        ]
    }
    scoreboard["queries"][16] = {"query_nr": 17, "status": "OK", "ratio": 100.0}
    assert check.compare(scoreboard, baseline) == 1


def test_baseline_nan_ratio_fails() -> None:
    """G10 C3-L-001: NaN must not green-exit (IEEE NaN is never > ceiling)."""
    check = _check_baseline_mod()
    scoreboard = {"queries": [{"query_nr": 1, "status": "OK", "ratio": float("nan")}]}
    baseline = {"queries": {"1": {"ceiling": 10.0}}}
    assert check.compare(scoreboard, baseline) == 1


def test_baseline_inf_ratio_fails() -> None:
    """G10 C3-L-001: +inf ratio fails closed."""
    check = _check_baseline_mod()
    scoreboard = {"queries": [{"query_nr": 1, "status": "OK", "ratio": float("inf")}]}
    baseline = {"queries": {"1": {"ceiling": 10.0}}}
    assert check.compare(scoreboard, baseline) == 1
