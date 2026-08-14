"""R-SQL-FUZZER — always-on smoke battery (200 queries, fixed seed 42).

Named oracle deliverable for D3. DuckDB is the differential oracle (same as TPC-H).
Determinism: seed is the fixed literal ``42`` (never time-based). Budget: <60s.

WRONG-RESULT on an unbanked query fails the smoke (signal). Banked repros are pinned
xfail-style by ``pin_id`` so the corpus stays red until a fix-forward slate flips them.
Engine product fixes are out of scope for this unit — bank + pin only.

Never touches AWS.
"""

from __future__ import annotations

import importlib
import sys
import time
import types
from pathlib import Path
from typing import Any

import pytest

duckdb = pytest.importorskip("duckdb")

from repark import ReparkSession  # noqa: E402

_FUZZ_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench" / "fuzz"
_REPROS_DIR = _FUZZ_DIR / "repros"

# Charter defaults — fixed literals, not env, for the always-on smoke pin.
SMOKE_SEED = 42
SMOKE_N = 200
SMOKE_BUDGET_S = 60.0


def _load_fuzz_package() -> Any:
    """Import bench/fuzz as a pseudo-package (bench is not under site-packages)."""
    package_name = "repark_fuzz_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_FUZZ_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.runner")


def _generator_mod() -> Any:
    _load_fuzz_package()
    return importlib.import_module("repark_fuzz_bench.generator")


def _datagen_mod() -> Any:
    _load_fuzz_package()
    return importlib.import_module("repark_fuzz_bench.datagen")


def _compare_mod() -> Any:
    _load_fuzz_package()
    return importlib.import_module("repark_fuzz_bench.compare")


def _bank_mod() -> Any:
    _load_fuzz_package()
    return importlib.import_module("repark_fuzz_bench.bank")


def _minimizer_mod() -> Any:
    _load_fuzz_package()
    return importlib.import_module("repark_fuzz_bench.minimizer")


# ---------------------------------------------------------------------------
# Unit pins — determinism + compare kernel + data invariants
# ---------------------------------------------------------------------------


def test_fuzz_seed_42_is_byte_identical_across_calls() -> None:
    """Same seed → byte-identical SQL list (HARD determinism contract)."""
    generator = _generator_mod()
    first = [query.sql for query in generator.generate_queries(SMOKE_SEED, 50)]
    second = [query.sql for query in generator.generate_queries(SMOKE_SEED, 50)]
    assert first == second
    assert first, "generator returned no queries"


def test_fuzz_different_seeds_diverge() -> None:
    generator = _generator_mod()
    a = [query.sql for query in generator.generate_queries(1, 30)]
    b = [query.sql for query in generator.generate_queries(2, 30)]
    assert a != b


def test_fuzz_null_density_floor() -> None:
    """Charter: NULL density ≥ 10% on generated fixtures."""
    datagen = _datagen_mod()
    database = datagen.generate_database(SMOKE_SEED)
    for table_name, table in database.tables.items():
        density = datagen.null_density_of(table)
        assert density >= 0.10, f"{table_name} null density {density:.3f} < 0.10"


def test_fuzz_no_nan_in_fixture() -> None:
    import math

    datagen = _datagen_mod()
    database = datagen.generate_database(SMOKE_SEED)
    for table in database.tables.values():
        for row in table.rows:
            for cell in row:
                if isinstance(cell, float):
                    assert math.isfinite(cell), f"non-finite float in fixture: {cell!r}"


def test_fuzz_compare_ltz_utc_matches_naive_wall() -> None:
    """TZ-4 PR-2: aware UTC instants compare equal to DuckDB's naive same wall."""
    import datetime as dt

    compare = _compare_mod()
    aware = dt.datetime(2020, 2, 12, 21, 30, tzinfo=dt.UTC)
    naive = dt.datetime(2020, 2, 12, 21, 30)
    assert compare.compare_result_sets([(aware,)], [(naive,)]).equal


def test_fuzz_compare_integer_exact_and_float_tol() -> None:
    compare = _compare_mod()
    assert compare.compare_result_sets([(1,)], [(1,)]).equal
    assert not compare.compare_result_sets([(1,)], [(2,)]).equal
    # Use non-integral floats so normalize does not collapse 1.0 → int.
    assert compare.compare_result_sets([(1.25,)], [(1.25 + 1e-7,)]).equal
    assert not compare.compare_result_sets([(1.25,)], [(1.25 + 1e-4,)]).equal


def test_fuzz_compare_decimal_not_collapsed_via_float() -> None:
    """C1-L-003: distinct Decimals that share a float() image must not soft-equal."""
    from decimal import Decimal

    compare = _compare_mod()
    # Both map to float 1.0 but are distinct Decimals (would mask via float()).
    left = Decimal("1.0000000000000000000000001")
    right = Decimal("1.0000000000000000000000002")
    assert left != right
    assert float(left) == float(right) == 1.0
    assert not compare.compare_result_sets([(left,)], [(right,)]).equal
    assert compare.compare_result_sets([(left,)], [(left,)]).equal


def test_fuzz_compare_int_vs_integral_float() -> None:
    """Integral-valued float normalizes to int → exact match (TPC-H bar)."""
    compare = _compare_mod()
    assert compare.compare_result_sets([(1,)], [(1.0,)]).equal
    # Near-integer float still soft-equals under 1e-6 rel (documented TPC-H bar).
    assert compare.compare_result_sets([(1,)], [(1.0000004,)]).equal
    assert not compare.compare_result_sets([(1,)], [(1.000002,)]).equal


def test_fuzz_compare_order_sensitive() -> None:
    compare = _compare_mod()
    left = [(1,), (2,)]
    right = [(2,), (1,)]
    assert compare.compare_result_sets(left, right, order_sensitive=False).equal
    assert not compare.compare_result_sets(left, right, order_sensitive=True).equal


def test_fuzz_aggregate_order_includes_ord_tie() -> None:
    """C1-L-001: aggregate LIMIT/ORDER BY must end with MIN(row_id) ord_tie."""
    generator = _generator_mod()
    found_with_limit = 0
    for query in generator.generate_queries(SMOKE_SEED, SMOKE_N):
        if query.spec.kind != "aggregate":
            continue
        if query.spec.limit is None and not query.spec.order_by:
            continue
        found_with_limit += 1
        assert "ord_tie" in query.spec.select_aliases, query.sql
        assert query.spec.order_by, query.sql
        assert query.spec.order_by[-1].expr == "ord_tie", query.sql
    assert found_with_limit > 0


def test_fuzz_minimizer_drops_order_leftmost_first() -> None:
    """C1-L-002 / C2-Q-001: first ORDER BY shrink removes the **leftmost** key.

    Mutation-proof: uses ``max_steps`` budget so only one ORDER BY item can be
    dropped after the LIMIT step; asserts the remaining keys equal ``original[1:]``.
    A rightmost-first policy would leave ``original[:-1]`` instead and fail this pin.
    """
    import copy

    minimizer = _minimizer_mod()
    generator = _generator_mod()
    datagen = _datagen_mod()

    query = None
    for candidate in generator.generate_queries(SMOKE_SEED, 80):
        if len(candidate.spec.order_by) >= 3 and candidate.spec.limit is not None:
            query = candidate
            break
    assert query is not None
    original_order = list(query.spec.order_by)
    database = datagen.generate_database(SMOKE_SEED)

    def execute(
        sql: str,
        db: Any,
        order_sensitive: bool,
    ) -> tuple[list[tuple[Any, ...]] | None, list[tuple[Any, ...]] | None, str | None, str | None]:
        del sql, db, order_sensitive
        return [(0,)], [(1,)], None, None

    # Budget: 1 (drop LIMIT) + 1 (first ORDER BY item) = 2 steps observed via
    # direct single-step trial matching minimizer policy (leftmost = original[1:]).
    trial_spec = copy.deepcopy(query.spec)
    trial_spec.limit = None
    trial_spec.order_by = list(trial_spec.order_by[1:])
    assert [item.expr for item in trial_spec.order_by] == [item.expr for item in original_order[1:]]
    # Rightmost-first counterfactual must differ for this pin to be mutation-proof.
    rightmost_first = [item.expr for item in original_order[:-1]]
    leftmost_first = [item.expr for item in original_order[1:]]
    assert leftmost_first != rightmost_first

    repro = minimizer.minimize_divergence(
        seed=SMOKE_SEED,
        query=query,
        database=database,
        execute=execute,
        max_steps=2,  # LIMIT drop + exactly one ORDER BY drop
    )
    assert repro is not None
    assert [item.expr for item in repro.spec.order_by] == leftmost_first


def test_fuzz_minimizer_join_drop_clears_where_on_dropped_table() -> None:
    """C6-L-001: dropping a join arm clears WHERE that referenced that table."""
    minimizer = _minimizer_mod()
    datagen = _datagen_mod()
    # Generator rarely puts right-table cols in WHERE (usually left). Synthesize.
    _load_fuzz_package()
    gen_mod = importlib.import_module("repark_fuzz_bench.generator")
    spec = gen_mod.QuerySpec(
        index=0,
        kind="join",
        from_table="t0",
        select_exprs=["t0.id", "t1.a"],
        select_aliases=["c0", "r0"],
        joins=[gen_mod.JoinClause(kind="INNER", right_table="t1", left_key="id", right_key="id")],
        where_sql="t1.a IS NOT NULL",
    )
    query = gen_mod.GeneratedQuery(index=0, sql=spec.render(), spec=spec)
    database = datagen.generate_database(SMOKE_SEED)

    def execute(
        sql: str,
        db: Any,
        order_sensitive: bool,
    ) -> tuple[list[tuple[Any, ...]] | None, list[tuple[Any, ...]] | None, str | None, str | None]:
        del sql, db, order_sensitive
        return [(0,)], [(1,)], None, None

    repro = minimizer.minimize_divergence(
        seed=SMOKE_SEED,
        query=query,
        database=database,
        execute=execute,
        max_steps=20,
    )
    assert repro is not None
    assert not repro.spec.joins
    assert repro.spec.where_sql is None or "t1." not in repro.spec.where_sql


def test_fuzz_minimizer_rejects_when_divergence_heals() -> None:
    """C1-Q-001: minimizer returns None when base pair no longer diverges."""
    minimizer = _minimizer_mod()
    generator = _generator_mod()
    datagen = _datagen_mod()
    query = generator.generate_queries(SMOKE_SEED, 1)[0]
    database = datagen.generate_database(SMOKE_SEED)

    def execute(
        sql: str,
        db: Any,
        order_sensitive: bool,
    ) -> tuple[list[tuple[Any, ...]] | None, list[tuple[Any, ...]] | None, str | None, str | None]:
        del sql, db, order_sensitive
        return [(1,)], [(1,)], None, None

    assert (
        minimizer.minimize_divergence(
            seed=SMOKE_SEED,
            query=query,
            database=database,
            execute=execute,
        )
        is None
    )


def test_fuzz_bank_roundtrip_minimized_fixture() -> None:
    """C1-Q-002: banked TABLE comments restore the minimized database."""
    import tempfile

    bank = _bank_mod()
    datagen = _datagen_mod()
    minimizer = _minimizer_mod()
    generator = _generator_mod()

    database = datagen.generate_database(0)
    # Shrink t0 to a single row for a distinctive fixture.
    t0 = database.tables["t0"]
    small = datagen.FuzzDatabase(
        seed=0,
        tables={
            "t0": datagen.FuzzTable(name="t0", columns=t0.columns, rows=t0.rows[:1]),
            "t1": database.tables["t1"],
            "t2": database.tables["t2"],
        },
    )
    query = generator.generate_queries(0, 1)[0]
    repro = minimizer.MinimizedRepro(
        seed=0,
        query_index=0,
        sql=query.sql,
        spec=query.spec,
        database=small,
        compare_message="test",
        repark_rows=[(1,)],
        duckdb_rows=[(2,)],
        steps=1,
    )

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        banked = bank.bank_repro(repro, repros_dir=out, sequence=1)
        text = Path(banked.path).read_text(encoding="utf-8")
        loaded = bank.load_minimized_database(text, seed=0)
        assert loaded is not None
        assert len(loaded.tables["t0"].rows) == 1
        assert loaded.tables["t0"].rows == small.tables["t0"].rows


def test_fuzz_long_pass_generator_deterministic_5000() -> None:
    """C1-Q-004: long-pass SQL stream is byte-identical (generator half of census)."""
    generator = _generator_mod()
    first = [q.sql for q in generator.generate_queries(SMOKE_SEED, 5000)]
    second = [q.sql for q in generator.generate_queries(SMOKE_SEED, 5000)]
    assert first == second
    assert len(first) == 5000


# ---------------------------------------------------------------------------
# Smoke run — 200 queries, seed 42, <60s
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fuzz_smoke_result() -> Any:
    """Run the smoke fuzzer once per module (shared cost)."""
    runner = _load_fuzz_package()
    # Do not bank into the source tree from CI smoke — banking is a CLI/long-pass job.
    # Repros already on disk are still loaded for xfail pins below.
    started = time.perf_counter()
    result = runner.run_fuzzer(
        seed=SMOKE_SEED,
        count=SMOKE_N,
        bank=False,
        minimize=False,
    )
    result.wall_s = time.perf_counter() - started
    return result


def test_fuzz_smoke_budget_under_60s(fuzz_smoke_result: Any) -> None:
    assert fuzz_smoke_result.wall_s < SMOKE_BUDGET_S, (
        f"smoke wall {fuzz_smoke_result.wall_s:.2f}s >= {SMOKE_BUDGET_S}s budget"
    )


def test_fuzz_smoke_seed_recorded(fuzz_smoke_result: Any) -> None:
    assert fuzz_smoke_result.seed == SMOKE_SEED
    assert fuzz_smoke_result.query_count == SMOKE_N


def test_fuzz_smoke_no_unbanked_wrong_results(fuzz_smoke_result: Any) -> None:
    """Any WRONG-RESULT must be in the banked corpus (xfail pins); else fail loud.

    During D3 infrastructure landing, minimize=False in smoke so the gate stays
    fast; banked corpus is produced by the long pass / CLI. Unbanked WRONG-RESULT
    fails so silent correctness regressions cannot hide.
    """
    bank = _bank_mod()
    banked = bank.list_banked_repros(_REPROS_DIR)
    banked_sql = {item.sql.strip() for item in banked}
    banked_indices = {(item.seed, item.query_index) for item in banked}

    unbanked: list[str] = []
    for outcome in fuzz_smoke_result.outcomes:
        if outcome.status != "WRONG-RESULT":
            continue
        key = (fuzz_smoke_result.seed, outcome.index)
        if outcome.sql.strip() in banked_sql or key in banked_indices:
            continue
        unbanked.append(f"index={outcome.index}: {outcome.message} sql={outcome.sql[:120]}")

    if unbanked:
        pytest.fail(
            "unbanked WRONG-RESULT (bank via run_fuzz.py --bank; engine fixes out of scope):\n"
            + "\n".join(unbanked[:20])
        )


def test_fuzz_smoke_error_rate_bounded(fuzz_smoke_result: Any) -> None:
    """ERROR is allowed (unsupported surface) but must not dominate the smoke.

    Bound: <50% ERROR. A generator that mostly emits unsupported SQL is not a
    useful differential harness. Tighten in follow-ups as the shared dialect grows.
    """
    total = max(1, fuzz_smoke_result.query_count)
    error_rate = fuzz_smoke_result.error_count / float(total)
    assert error_rate < 0.50, (
        f"ERROR rate {error_rate:.2%} >= 50% — generator too far outside shared dialect; "
        f"census={fuzz_smoke_result.census()}"
    )


def test_fuzz_smoke_has_some_ok(fuzz_smoke_result: Any) -> None:
    assert fuzz_smoke_result.ok_count > 0, (
        f"zero OK queries — harness or fixture broken; census={fuzz_smoke_result.census()}"
    )


# ---------------------------------------------------------------------------
# Banked repro xfail pins (empty corpus → no parametrize cases)
# ---------------------------------------------------------------------------


def _banked_repro_cases() -> list[Any]:
    if not _REPROS_DIR.is_dir():
        return []
    # Late import-safe: only path scan here; runner import needs duckdb/repark.
    pattern_files = sorted(_REPROS_DIR.glob("*.sql"))
    return pattern_files


@pytest.mark.parametrize("repro_path", _banked_repro_cases(), ids=lambda path: path.name)
def test_fuzz_banked_repro_still_diverges_or_xfail(repro_path: Path) -> None:
    """Each banked repro is an xfail-style pin naming its tracking seed row.

    While the divergence remains, the pin expects WRONG-RESULT (documents the bug).
    When a fix-forward slate resolves it, this test goes red → flip to OK pin + ledger.
    """
    runner = _load_fuzz_package()
    datagen = _datagen_mod()
    compare = _compare_mod()

    text = repro_path.read_text(encoding="utf-8")
    # Parse seed from filename stem ``<seed>-<n>``.
    stem = repro_path.stem
    seed_str, _, seq_str = stem.partition("-")
    seed = int(seed_str)
    pin_id = f"fuzz-{seed}-{seq_str}"

    sql_lines = [line for line in text.splitlines() if not line.startswith("--") and line.strip()]
    sql = "\n".join(sql_lines).strip()
    assert sql, f"banked repro {repro_path} has no SQL body"

    bank = _bank_mod()
    # Prefer the minimized TABLE fixture banked with the repro (C1-Q-002). Fall back
    # to the full seed fixture only for legacy files without a TABLE section.
    minimized = bank.load_minimized_database(text, seed=seed)
    database = minimized if minimized is not None else datagen.generate_database(seed)
    repark_session = runner._open_repark(database)
    duck_conn = runner._open_duckdb(database)
    try:
        try:
            repark_rows = runner._repark_collect(repark_session, sql)
            repark_err = None
        except Exception as exc:
            repark_rows = None
            repark_err = str(exc)
        try:
            duck_rows = runner._duck_collect(duck_conn, sql)
            duck_err = None
        except Exception as exc:
            duck_rows = None
            duck_err = str(exc)
    finally:
        runner._close_repark(repark_session)
        runner._close_duckdb(duck_conn)

    if repark_err is not None or duck_err is not None:
        pytest.xfail(
            f"{pin_id}: banked repro now ERROR status "
            f"(repark={repark_err!r} duck={duck_err!r}) "
            "— reclassify in ledger; not silently green"
        )

    assert repark_rows is not None and duck_rows is not None
    # Prefer bank header (C7-L-001); fall back to SQL text heuristic for legacy files.
    has_order_header = bank._comment_int(text, "has_order_by")
    if has_order_header is not None:
        order_sensitive = bool(has_order_header)
    else:
        order_sensitive = "ORDER BY" in sql.upper()
    result = compare.compare_result_sets(
        repark_rows,
        duck_rows,
        order_sensitive=order_sensitive,
    )
    if result.equal:
        pytest.fail(
            f"{pin_id}: banked repro no longer diverges — fix-forward may have landed; "
            "remove repro + flip pin (do not leave a stale red-corpus entry)"
        )
    # Still diverges: pin holds (xfail-style documentation of the open bug).
    assert not result.equal


# ---------------------------------------------------------------------------
# Session hygiene smoke (createDataFrame path used by the fuzzer)
# ---------------------------------------------------------------------------


def test_fuzz_repark_registers_temp_views() -> None:
    datagen = _datagen_mod()
    runner = _load_fuzz_package()
    database = datagen.generate_database(0)
    session = runner._open_repark(database)
    try:
        rows = runner._repark_collect(session, "SELECT COUNT(*) AS c FROM t0")
        assert rows and rows[0][0] == len(database.table("t0").rows)
    finally:
        runner._close_repark(session)
        # Clear process-wide session registry (conftest also does this).
        from repark.spark.session import _reset_active_session_for_tests

        _reset_active_session_for_tests()


def test_default_seed_constant_is_42() -> None:
    generator = _generator_mod()
    assert generator.DEFAULT_SEED == 42
    # Silence unused import lint paths for ReparkSession in collection.
    assert ReparkSession is not None


def test_fuzz_bank_sequence_continues_and_refuses_overwrite() -> None:
    """C4-L-001: next sequence scans disk; bank_repro refuses clobber."""
    import tempfile

    bank = _bank_mod()
    datagen = _datagen_mod()
    minimizer = _minimizer_mod()
    generator = _generator_mod()
    database = datagen.generate_database(0)
    query = generator.generate_queries(0, 1)[0]

    def make_repro() -> Any:
        return minimizer.MinimizedRepro(
            seed=0,
            query_index=0,
            sql=query.sql,
            spec=query.spec,
            database=database,
            compare_message="m",
            repark_rows=[(1,)],
            duckdb_rows=[(2,)],
            steps=0,
        )

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        assert bank.next_bank_sequence(out, seed=0) == 1
        bank.bank_repro(make_repro(), repros_dir=out, sequence=1)
        assert bank.next_bank_sequence(out, seed=0) == 2
        try:
            bank.bank_repro(make_repro(), repros_dir=out, sequence=1)
            raise AssertionError("expected FileExistsError on overwrite")
        except FileExistsError:
            pass


def test_fuzz_corpus_index_lists_preexisting_repros() -> None:
    """C4-L-002: corpus_index.json includes on-disk repros from prior runs."""
    import json
    import tempfile

    bank = _bank_mod()
    runner = _load_fuzz_package()
    datagen = _datagen_mod()
    minimizer = _minimizer_mod()
    generator = _generator_mod()
    database = datagen.generate_database(0)
    query = generator.generate_queries(0, 1)[0]
    repro = minimizer.MinimizedRepro(
        seed=0,
        query_index=0,
        sql=query.sql,
        spec=query.spec,
        database=database,
        compare_message="prior",
        repark_rows=[(1,)],
        duckdb_rows=[(2,)],
        steps=0,
    )
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        bank.bank_repro(repro, repros_dir=out, sequence=1)
        # Run with bank=True but no new wrongs — index must still list the prior file.
        runner.run_fuzzer(seed=0, count=3, bank=True, repros_dir=out, minimize=False)
        payload = json.loads((out / "corpus_index.json").read_text(encoding="utf-8"))
        assert payload["corpus_count"] == 1
        assert payload["repros"][0]["pin_id"] == "fuzz-0-1"


def test_fuzz_json_artifact_records_seed() -> None:
    """C7-Q-001: census / to_json_obj always carry the resolved seed."""
    runner = _load_fuzz_package()
    result = runner.run_fuzzer(seed=SMOKE_SEED, count=3, bank=False, minimize=False)
    payload = result.to_json_obj()
    assert payload["seed"] == SMOKE_SEED
    assert payload["census"]["seed"] == SMOKE_SEED
    assert payload["environment"]["seed"] == str(SMOKE_SEED)


def test_fuzz_bank_header_records_has_order_by() -> None:
    """C7-L-001: bank header carries has_order_by for pin replay."""
    import tempfile

    bank = _bank_mod()
    datagen = _datagen_mod()
    minimizer = _minimizer_mod()
    generator = _generator_mod()
    database = datagen.generate_database(0)
    query = None
    for candidate in generator.generate_queries(0, 40):
        if candidate.spec.has_order_by:
            query = candidate
            break
    assert query is not None
    repro = minimizer.MinimizedRepro(
        seed=0,
        query_index=query.index,
        sql=query.sql,
        spec=query.spec,
        database=database,
        compare_message="m",
        repark_rows=[(1,)],
        duckdb_rows=[(2,)],
        steps=0,
    )
    with tempfile.TemporaryDirectory() as tmp:
        banked = bank.bank_repro(repro, repros_dir=Path(tmp), sequence=1)
        text = Path(banked.path).read_text(encoding="utf-8")
        assert bank._comment_int(text, "has_order_by") == 1


def test_fuzz_resolve_seed_rejects_negative() -> None:
    """C3-L-001 / C3-Q-002: negative seeds break bank filenames — reject early."""
    runner = _load_fuzz_package()
    try:
        runner.resolve_seed(-1)
        raise AssertionError("expected ValueError for negative seed")
    except ValueError as exc:
        assert "seed must be >= 0" in str(exc)


def test_fuzz_bank_compare_message_single_line() -> None:
    """C3-Q-001: multiline compare messages must not split bank headers."""
    import tempfile

    bank = _bank_mod()
    datagen = _datagen_mod()
    minimizer = _minimizer_mod()
    generator = _generator_mod()
    database = datagen.generate_database(0)
    query = generator.generate_queries(0, 1)[0]
    repro = minimizer.MinimizedRepro(
        seed=0,
        query_index=0,
        sql=query.sql,
        spec=query.spec,
        database=database,
        compare_message="row mismatch\nextra detail",
        repark_rows=[(1,)],
        duckdb_rows=[(2,)],
        steps=0,
    )
    with tempfile.TemporaryDirectory() as tmp:
        bank.bank_repro(repro, repros_dir=Path(tmp), sequence=1)
        listed = bank.list_banked_repros(Path(tmp))
        assert len(listed) == 1
        assert "\n" not in listed[0].compare_message
        assert "row mismatch" in listed[0].compare_message
        assert "extra detail" in listed[0].compare_message
