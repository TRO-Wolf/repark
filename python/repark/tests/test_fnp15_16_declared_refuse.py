"""FNP-15/16 — declared-absent Spark functions refuse loudly.

pins: fnp-15-16/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-012, C-013,
C-016, C-017
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from repark.errors import UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom
from repark.spark.session import ReparkSession

FNP15_NAMES: tuple[str, ...] = (
    "java_method",
    "reflect",
    "try_reflect",
    "unwrap_udt",
    "input_file_block_start",
    "input_file_block_length",
)

FNP15_NEEDLES: dict[str, tuple[str, ...]] = {
    "java_method": ("unreachable", "Java class", "reflection"),
    "reflect": ("unreachable", "CallMethodViaReflection"),
    "try_reflect": ("unreachable", "exception-to-NULL"),
    "unwrap_udt": ("unreachable", "UserDefinedType"),
    "input_file_block_start": ("unreachable", "InputFileBlockHolder", "input_file_name"),
    "input_file_block_length": ("unreachable", "InputFileBlockHolder", "input_file_name"),
}

STILL_ABSENT: str = "aes_encrypt"


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("fnp-15-16-refuse").getOrCreate()
    yield session
    session.stop()


def _assert_declared_refusal(exc: BaseException, name: str) -> None:
    text = str(exc)
    assert isinstance(exc, UnsupportedOperationException), type(exc)
    assert name in text, text
    for needle in FNP15_NEEDLES[name]:
        assert needle in text, f"missing {needle!r} in {text}"
    assert "unreachable" in text, text
    assert "deferred by cost" not in text, text


def _sql_call(name: str) -> str:
    return f"SELECT {name}(1)"


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_facade_attribute_refuses_with_registry_reason(name: str) -> None:
    """Facade Column API raises UnsupportedOperationException, not AttributeError."""
    fn: Callable[..., Any] = getattr(F, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_declared_refusal(caught.value, name)


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_sql_functions_reexport_refuses(name: str) -> None:
    """sed-swap path ``repark.spark.sql.functions`` carries the same stub."""
    from repark.spark.sql import functions as sql_functions

    fn: Callable[..., Any] = getattr(sql_functions, name)
    with pytest.raises(UnsupportedOperationException) as caught:
        fn("x")
    _assert_declared_refusal(caught.value, name)
    assert getattr(sql_functions, name) is getattr(F, name)


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_spark_sql_door_refuses(name: str, spark: ReparkSession) -> None:
    """Spark SQL door parse-altitude refusal names the same mechanism."""
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(_sql_call(name)).collect()
    _assert_declared_refusal(caught.value, name)


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_ansi_sql_door_refuses(name: str) -> None:
    """ANSI door parse-altitude refusal names the same mechanism."""
    import repark

    with pytest.raises(UnsupportedOperationException) as caught:
        repark.sql(_sql_call(name))
    _assert_declared_refusal(caught.value, name)


@pytest.mark.parametrize("name", FNP15_NAMES)
def test_expr_fragment_refuses(name: str) -> None:
    """``F.expr`` bypasses the router; the fragment valve must still fire."""
    with pytest.raises(UnsupportedOperationException) as caught:
        F.expr(f"{name}(1)")
    _assert_declared_refusal(caught.value, name)


def test_still_missing_name_stays_attribute_error() -> None:
    """FNP-Z owns wholesale __all__ completion; names outside the 62 stay absent."""
    with pytest.raises(AttributeError):
        getattr(F, STILL_ABSENT)


def test_live_function_and_default_select_still_plan(spark: ReparkSession) -> None:
    """New refusals must not change a live function or ``SELECT 1``."""
    import pyarrow as pa

    import repark

    table = spark.createDataFrame([(1,)], "x int").select(F.abs("x")).toArrow()
    assert table.column(0).to_pylist() == [1]
    ansi = repark.sql("SELECT 1 AS one").to_arrow()
    assert pa.types.is_integer(ansi.schema.field("one").type)
    assert ansi.column("one").to_pylist() == [1]


def test_registry_wording_distinguishes_unreachable_from_deferred_cost() -> None:
    """FNP-15 sections say unreachable; they must not classify as deferred-by-cost."""
    from pathlib import Path

    registry = Path(__file__).resolve().parents[3] / "docs" / "spark-sql-iceberg-parity.md"
    text = registry.read_text(encoding="utf-8")
    start = text.index("## 9. Declared-absent Spark functions")
    body = text[start:]
    next_heading = body.find("\n## ", 4)
    section = body if next_heading < 0 else body[:next_heading]
    for name in FNP15_NAMES:
        heading = f"### FNP-15-{name}"
        assert heading in section, f"missing registry section {heading}"
        chunk_start = section.index(heading)
        rest = section[chunk_start + len(heading) :]
        next_chunk = rest.find("\n### ")
        chunk = rest if next_chunk < 0 else rest[:next_chunk]
        assert "unreachable" in chunk, heading
        assert "deferred by cost" not in chunk, heading
        classification = chunk.lower().replace("unsupportedoperationexception", "")
        assert "unsupported" not in classification, heading


def test_roster_counts_fnp15_six() -> None:
    """FNP-15 is exactly the six unreachable names."""
    assert len(FNP15_NAMES) == 6
    assert len(set(FNP15_NAMES)) == 6
