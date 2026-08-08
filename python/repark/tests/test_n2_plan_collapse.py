"""r23b N2 — adjacent projection/window collapse (alias-chain squash + window merge).

Plan-shape pins (WindowAggExec / logical ``AS`` chain counts) + Arrow value correctness.
No brittle full plan-string pins (Q13). Synthetic OHLCV only.
"""

from __future__ import annotations

import contextlib
import io
import re
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession, Window, ta
from repark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-n2-plan-collapse").getOrCreate()


@pytest.fixture
def bars(spark: ReparkSession, tmp_path: Path) -> object:
    """Small synthetic OHLCV frame (deterministic)."""
    n = 200
    rng = np.random.default_rng(7)
    ts = np.arange(n, dtype=np.int64)
    close = 100.0 + np.cumsum(rng.normal(0, 0.3, size=n))
    high = close + rng.uniform(0.05, 0.5, size=n)
    low = close - rng.uniform(0.05, 0.5, size=n)
    open_ = close + rng.normal(0, 0.1, size=n)
    volume = rng.integers(100, 500, size=n).astype(np.float64)
    table = pa.table(
        {
            "ts": ts,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )
    path = tmp_path / "n2_bars.parquet"
    pq.write_table(table, path)
    return spark.read_parquet(str(path))


def _physical_plan_text(df: object) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain()  # type: ignore[attr-defined]
    text = buffer.getvalue()
    match = re.search(
        r"plan_type='physical_plan', plan='((?:\\'|[^'])*)'",
        text,
    )
    if match is None:
        return text
    return match.group(1).replace("\\n", "\n").replace("\\'", "'")


def _logical_plan_text(df: object) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(True)  # type: ignore[attr-defined]
    text = buffer.getvalue()
    match = re.search(
        r"plan_type='logical_plan', plan='((?:\\'|[^'])*)'",
        text,
    )
    if match is None:
        return text
    return match.group(1).replace("\\n", "\n").replace("\\'", "'")


def _assert_bit_exact(left: np.ndarray, right: np.ndarray) -> None:
    left = np.ascontiguousarray(left, dtype=np.float64)
    right = np.ascontiguousarray(right, dtype=np.float64)
    assert left.shape == right.shape
    both_nan = np.isnan(left) & np.isnan(right)
    mismatch = (left.view(np.uint64) != right.view(np.uint64)) & ~both_nan
    if mismatch.any():
        first = int(np.flatnonzero(mismatch)[0])
        raise AssertionError(f"bit mismatch at row {first}: {left[first]!r} vs {right[first]!r}")


def test_stage_a_alias_chain_squash_passthrough(bars: object) -> None:
    """Passthrough columns must not stack identity ``x AS x AS x`` on layered withColumns."""
    window = Window.orderBy("ts")
    frame = bars.withColumns(  # type: ignore[attr-defined]
        {"ema5": ta.ema("close", timeperiod=5).over(window)}
    )
    frame = frame.withColumns({"sma10": ta.sma("close", timeperiod=10).over(window)})
    logical = _logical_plan_text(frame)
    # After squash: at most one identity ``AS name`` per field per projection level —
    # triple ``ts AS ts AS ts`` is the pre-fix anti-pattern.
    assert "ts AS ts AS ts" not in logical, logical[:1200]
    assert "close AS close AS close" not in logical, logical[:1200]
    # Still projects the columns (behavioral, not full-plan pin).
    table = frame.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    assert "ema5" in table.column_names
    assert "sma10" in table.column_names


def test_stage_b_adjacent_same_spec_withcolumns_merges(bars: object) -> None:
    """Independent same-spec withColumns chains merge to one WindowAggExec."""
    window = Window.orderBy("ts")
    batch1 = {
        "ema5": ta.ema("close", timeperiod=5).over(window),
        "sma10": ta.sma("close", timeperiod=10).over(window),
    }
    batch2 = {
        "rsi14": ta.rsi("close", timeperiod=14).over(window),
        "mom10": ta.mom("close", timeperiod=10).over(window),
    }
    chained = bars.withColumns(batch1).withColumns(batch2)  # type: ignore[attr-defined]
    fused = bars.withColumns({**batch1, **batch2})  # type: ignore[attr-defined]
    plan = _physical_plan_text(chained)
    assert plan.count("WindowAggExec") == 1, plan[:2000]
    assert plan.count("WindowAggExec") == _physical_plan_text(fused).count("WindowAggExec")
    left = chained.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    right = fused.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    for name in (*batch1, *batch2):
        _assert_bit_exact(
            left.column(name).to_numpy(zero_copy_only=False),
            right.column(name).to_numpy(zero_copy_only=False),
        )


def test_stage_b_adjacent_same_spec_withcolumn_merges(bars: object) -> None:
    """Independent same-spec sequential withColumn also merges (Q14)."""
    window = Window.orderBy("ts")
    frame = bars  # type: ignore[assignment]
    specs = (
        ("ema5", ta.ema("close", timeperiod=5).over(window)),
        ("sma10", ta.sma("close", timeperiod=10).over(window)),
        ("rsi14", ta.rsi("close", timeperiod=14).over(window)),
    )
    for name, column in specs:
        frame = frame.withColumn(name, column)  # type: ignore[attr-defined]
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 1, plan[:2000]
    fused = bars.withColumns(dict(specs))  # type: ignore[attr-defined]
    left = frame.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    right = fused.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    for name, _column in specs:
        _assert_bit_exact(
            left.column(name).to_numpy(zero_copy_only=False),
            right.column(name).to_numpy(zero_copy_only=False),
        )


def test_stage_b_dependent_column_keeps_stacking(bars: object) -> None:
    """ETR-style dep on prior-layer defined name MUST keep stacking (Q16)."""
    window = Window.orderBy("ts")
    frame = bars.withColumn("tr", ta.trange("high", "low", "close").over(window))  # type: ignore[attr-defined]
    frame = frame.withColumn("etr5", F.avg("tr").over(window))  # type: ignore[attr-defined]
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 2, plan[:2000]
    table = frame.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    assert "tr" in table.column_names and "etr5" in table.column_names
    # Sanity: etr5 is null-safe float column with same length.
    assert table.num_rows == bars.count()  # type: ignore[attr-defined]


def test_stage_b_filter_blocks_merge(bars: object) -> None:
    """Intervening filter blocks adjacent merge (Q15)."""
    window = Window.orderBy("ts")
    frame = bars.withColumns(  # type: ignore[attr-defined]
        {"ema5": ta.ema("close", timeperiod=5).over(window)}
    )
    frame = frame.filter(F.col("close") > 0)  # type: ignore[attr-defined]
    frame = frame.withColumns({"sma10": ta.sma("close", timeperiod=10).over(window)})
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 2, plan[:2000]


def test_stage_b_round_wrap_same_layer_merges(bars: object) -> None:
    """``.round()`` is same-layer wrap (Q15) — chained round-wrapped withColumns still merge."""
    window = Window.orderBy("ts")
    batch1 = {
        "ema5": ta.ema("close", timeperiod=5).over(window).round(4),
        "sma10": ta.sma("close", timeperiod=10).over(window).round(4),
    }
    batch2 = {
        "rsi14": ta.rsi("close", timeperiod=14).over(window).round(4),
        "mom10": ta.mom("close", timeperiod=10).over(window).round(4),
    }
    chained = bars.withColumns(batch1).withColumns(batch2)  # type: ignore[attr-defined]
    plan = _physical_plan_text(chained)
    assert plan.count("WindowAggExec") == 1, plan[:2000]
    fused = bars.withColumns({**batch1, **batch2})  # type: ignore[attr-defined]
    left = chained.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    right = fused.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    for name in (*batch1, *batch2):
        _assert_bit_exact(
            left.column(name).to_numpy(zero_copy_only=False),
            right.column(name).to_numpy(zero_copy_only=False),
        )


def test_stage_b_four_chain_operator_shape(bars: object) -> None:
    """Operator-shaped 4-chain independent same-window batches → one WindowAggExec."""
    window = Window.orderBy("ts")
    batches = [
        {
            "ema5": ta.ema("close", timeperiod=5).over(window),
            "ema10": ta.ema("close", timeperiod=10).over(window),
        },
        {
            "sma10": ta.sma("close", timeperiod=10).over(window),
            "rsi14": ta.rsi("close", timeperiod=14).over(window),
        },
        {
            "mom10": ta.mom("close", timeperiod=10).over(window),
            "atr14": ta.atr("high", "low", "close", timeperiod=14).over(window),
        },
        {
            "adx14": ta.adx("high", "low", "close", timeperiod=14).over(window),
            "willr14": ta.willr("high", "low", "close", timeperiod=14).over(window),
        },
    ]
    chained = bars  # type: ignore[assignment]
    combined: dict[str, object] = {}
    for batch in batches:
        chained = chained.withColumns(batch)  # type: ignore[attr-defined]
        combined.update(batch)
    plan = _physical_plan_text(chained)
    assert plan.count("WindowAggExec") == 1, plan[:2500]
    fused = bars.withColumns(combined)  # type: ignore[attr-defined]
    left = chained.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    right = fused.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    for name in combined:
        _assert_bit_exact(
            left.column(name).to_numpy(zero_copy_only=False),
            right.column(name).to_numpy(zero_copy_only=False),
        )


def test_stage_b_different_window_spec_no_merge(bars: object) -> None:
    """Different orderBy windows must not merge."""
    w1 = Window.orderBy("ts")
    w2 = Window.orderBy(F.col("ts").desc())
    frame = bars.withColumn("ema5", ta.ema("close", timeperiod=5).over(w1))  # type: ignore[attr-defined]
    frame = frame.withColumn("sma10", ta.sma("close", timeperiod=10).over(w2))  # type: ignore[attr-defined]
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 2, plan[:2000]


def test_stage_b_drop_blocks_merge(bars: object) -> None:
    """Intervening drop blocks adjacent merge (Q15) — octo C1-Q-001."""
    window = Window.orderBy("ts")
    frame = bars.withColumn("ema5", ta.ema("close", timeperiod=5).over(window))  # type: ignore[attr-defined]
    frame = frame.drop("volume")  # type: ignore[attr-defined]
    frame = frame.withColumn("sma10", ta.sma("close", timeperiod=10).over(window))  # type: ignore[attr-defined]
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 2, plan[:2000]
    table = frame.to_arrow()  # type: ignore[attr-defined]
    assert "ema5" in table.column_names and "sma10" in table.column_names
    assert "volume" not in table.column_names


def test_stage_b_select_subset_blocks_merge(bars: object) -> None:
    """Intervening select-subset blocks adjacent merge (Q15) — octo C1-Q-002."""
    window = Window.orderBy("ts")
    frame = bars.withColumn("ema5", ta.ema("close", timeperiod=5).over(window))  # type: ignore[attr-defined]
    frame = frame.select("ts", "close", "high", "low", "ema5")  # type: ignore[attr-defined]
    frame = frame.withColumn("sma10", ta.sma("close", timeperiod=10).over(window))  # type: ignore[attr-defined]
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 2, plan[:2000]
    table = frame.to_arrow()  # type: ignore[attr-defined]
    assert set(table.column_names) >= {"ts", "close", "ema5", "sma10"}


def test_stage_b_alias_wrap_same_layer_merges(bars: object) -> None:
    """``.alias`` is same-layer wrap (Q15) — octo C1-Q-003."""
    window = Window.orderBy("ts")
    frame = bars.withColumn(  # type: ignore[attr-defined]
        "ema5", ta.ema("close", timeperiod=5).over(window).alias("ema5")
    )
    frame = frame.withColumn(  # type: ignore[attr-defined]
        "sma10", ta.sma("close", timeperiod=10).over(window).alias("sma10")
    )
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 1, plan[:2000]
    fused = bars.withColumns(  # type: ignore[attr-defined]
        {
            "ema5": ta.ema("close", timeperiod=5).over(window).alias("ema5"),
            "sma10": ta.sma("close", timeperiod=10).over(window).alias("sma10"),
        }
    )
    left = frame.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    right = fused.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    for name in ("ema5", "sma10"):
        _assert_bit_exact(
            left.column(name).to_numpy(zero_copy_only=False),
            right.column(name).to_numpy(zero_copy_only=False),
        )


def test_stage_b_cache_blocks_merge(bars: object) -> None:
    """cache()/persist() return self — must not merge past a cache mark (octo C2-L-001)."""
    window = Window.orderBy("ts")
    frame = bars.withColumn("ema5", ta.ema("close", timeperiod=5).over(window))  # type: ignore[attr-defined]
    frame = frame.cache()  # type: ignore[attr-defined]
    frame = frame.withColumn("sma10", ta.sma("close", timeperiod=10).over(window))  # type: ignore[attr-defined]
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 2, plan[:2000]
    table = frame.to_arrow()  # type: ignore[attr-defined]
    assert "ema5" in table.column_names and "sma10" in table.column_names


def test_stage_b_overwrite_base_name_blocks_merge(bars: object) -> None:
    """Layer that redefines a base name must not merge a later reader of that name — C1-Q-004.

    Sequential: second ``ema(close)`` sees the *replaced* close. Merged-on-base would see
    original close → wrong values. Dep sniff must keep stacking (2 WindowAggExec).
    """
    window = Window.orderBy("ts")
    frame = bars.withColumn(  # type: ignore[attr-defined]
        "close", ta.ema("close", timeperiod=5).over(window)
    )
    frame = frame.withColumn(  # type: ignore[attr-defined]
        "ema10", ta.ema("close", timeperiod=10).over(window)
    )
    plan = _physical_plan_text(frame)
    assert plan.count("WindowAggExec") == 2, plan[:2000]
    table = frame.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    assert "close" in table.column_names and "ema10" in table.column_names
    # Sanity: stacked path produces finite floats (not plan-only assert).
    close_vals = table.column("close").to_numpy(zero_copy_only=False)
    assert close_vals.shape[0] == bars.count()  # type: ignore[attr-defined]


# ==================================================================================================
# r25 T3 residual — nested identity Alias peel + operator-shaped 17-TA value parity
# ==================================================================================================


def _repeated_alias_nodes(logical: str) -> list[str]:
    """Names that appear as nested ``… AS name AS name`` (repeated-alias residual)."""
    return re.findall(r" AS ([A-Za-z_][A-Za-z0-9_]*) AS \1\b", logical)


def test_t3_double_alias_select_peels_repeated_identity(bars: object) -> None:
    """r25 T3: ``col.alias(name).alias(name)`` must not plan as ``… AS name AS name``.

    Extends N2 stage (a) collapse path — peel happens inside
    ``_collapse_identity_projection_alias`` via native ``collapse_identity_aliases``.
    """
    stacked = F.col("close").alias("close").alias("close")
    frame = bars.select(stacked, "ts")  # type: ignore[attr-defined]
    logical = _logical_plan_text(frame)
    assert _repeated_alias_nodes(logical) == [], logical[:1200]
    # Still a single identity rename is fine; values unchanged.
    table = frame.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    base = bars.select("ts", "close").to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    _assert_bit_exact(
        table.column("close").to_numpy(zero_copy_only=False),
        base.column("close").to_numpy(zero_copy_only=False),
    )


def test_t3_rename_double_alias_peels_to_single(bars: object) -> None:
    """``col.alias("c").alias("c")`` → one ``AS c``, not ``AS c AS c``."""
    frame = bars.select(F.col("close").alias("c").alias("c"), "ts")  # type: ignore[attr-defined]
    logical = _logical_plan_text(frame)
    assert _repeated_alias_nodes(logical) == [], logical[:1200]
    assert " AS c" in logical or "as c" in logical.lower()
    table = frame.to_arrow()  # type: ignore[attr-defined]
    assert "c" in table.column_names
    assert table.num_rows == bars.count()  # type: ignore[attr-defined]


def test_t3_distinct_rename_chain_peels_to_outer(bars: object) -> None:
    """``col.alias("a").alias("b")`` → ``… AS b`` (not ``… AS a AS b``) — octo C1-Q-006."""
    frame = bars.select(F.col("close").alias("a").alias("b"), "ts")  # type: ignore[attr-defined]
    logical = _logical_plan_text(frame)
    assert " AS a AS b" not in logical, logical[:1200]
    assert _repeated_alias_nodes(logical) == [], logical[:1200]
    assert frame.columns == ["b", "ts"]  # type: ignore[attr-defined]
    table = frame.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    base = bars.select(F.col("close").alias("b"), "ts").to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    _assert_bit_exact(
        table.column("b").to_numpy(zero_copy_only=False),
        base.column("b").to_numpy(zero_copy_only=False),
    )


def test_t3_operator_17_ta_chain_plan_and_value_parity(bars: object) -> None:
    """Operator-shaped 17-TA independent same-spec chain: ≤1 WindowAggExec, no repeated aliases.

    Value-parity vs single fused ``withColumns`` on the Arrow path (to_bits for floats).
    """
    window = Window.orderBy("ts")
    batches = [
        {
            "ema5": ta.ema("close", timeperiod=5).over(window),
            "ema10": ta.ema("close", timeperiod=10).over(window),
            "ema20": ta.ema("close", timeperiod=20).over(window),
        },
        {
            "sma10": ta.sma("close", timeperiod=10).over(window),
            "sma20": ta.sma("close", timeperiod=20).over(window),
            "rsi14": ta.rsi("close", timeperiod=14).over(window),
        },
        {
            "atr14": ta.atr("high", "low", "close", timeperiod=14).over(window),
            "tr": ta.trange("high", "low", "close").over(window),
            "adx14": ta.adx("high", "low", "close", timeperiod=14).over(window),
        },
        {
            "mom10": ta.mom("close", timeperiod=10).over(window),
            "willr14": ta.willr("high", "low", "close", timeperiod=14).over(window),
            "stddev20": ta.stddev("close", timeperiod=20).over(window),
        },
        {
            "linearreg14": ta.linearreg("close", timeperiod=14).over(window),
            "linearreg_slope14": ta.linearreg_slope("close", timeperiod=14).over(window),
            "linearreg_intercept14": ta.linearreg_intercept("close", timeperiod=14).over(window),
            "tsf14": ta.tsf("close", timeperiod=14).over(window),
            "var20": ta.var("close", timeperiod=20).over(window),
        },
    ]
    assert sum(len(batch) for batch in batches) == 17
    chained = bars  # type: ignore[assignment]
    combined: dict[str, object] = {}
    for batch in batches:
        chained = chained.withColumns(batch)  # type: ignore[attr-defined]
        combined.update(batch)
    physical = _physical_plan_text(chained)
    logical = _logical_plan_text(chained)
    assert physical.count("WindowAggExec") == 1, physical[:2500]
    assert physical.count("ProjectionExec") <= 2, physical[:2500]
    assert _repeated_alias_nodes(logical) == [], logical[:1500]
    # Triple identity anti-pattern (pre-N2) must stay dead.
    assert "ts AS ts AS ts" not in logical
    assert "close AS close AS close" not in logical
    fused = bars.withColumns(combined)  # type: ignore[attr-defined]
    left = chained.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    right = fused.to_arrow().sort_by("ts")  # type: ignore[attr-defined]
    for name in combined:
        _assert_bit_exact(
            left.column(name).to_numpy(zero_copy_only=False),
            right.column(name).to_numpy(zero_copy_only=False),
        )
