from __future__ import annotations

import json
import re
import shutil
import struct
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import _sum_dec_i128wrap_1_cells as cells
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.window import Window

ORACLE = json.loads(Path(__file__).with_name("sum_dec_i128wrap_1_spark_oracle.json").read_text())
KEYS = sorted(ORACLE["cells"]["false"])
SPREAD = cells.spread_sql()
SPREAD_SETS = sorted({(key.split("/")[0], key.split("/")[-1]) for key in SPREAD})
CONDITION = re.compile(r"\[([A-Z_]+)")
WRAP = 2**128


@pytest.fixture(scope="module", params=["false", "true"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[str, dict[str, Any], Path]]:
    root = tmp_path_factory.mktemp(f"sum-{request.param}")
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-sum-dec-i128wrap-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    cells.make_tables(session, str(root))
    found = cells.measure_all(session, sf, Window, "lc", "repark")
    session.stop()
    _reset_active_session_for_tests()
    yield request.param, found, root / "wh"


def condition(value: str) -> str | None:
    match = CONDITION.search(value)
    return match.group(1) if match else None


def assert_as_spark(found: Any, want: Any, key: str) -> None:
    if isinstance(want, str):
        assert isinstance(found, str), (key, found, want)
        assert condition(found) == condition(want), (key, found, want)
        return
    if isinstance(want, dict) and "statement" in want:
        assert isinstance(found, dict), (key, found)
        if want["statement"] == "ok":
            assert found["statement"] == "ok", (key, found, want)
        else:
            assert condition(found["statement"]) == condition(want["statement"]), (key, found)
        if isinstance(want["rows"], str) and condition(want["rows"]) == "TABLE_OR_VIEW_NOT_FOUND":
            assert isinstance(found["rows"], str), (key, found)
            assert "not found" in found["rows"], (key, found)
            return
        assert_as_spark(found["rows"], want["rows"], key)
        return
    assert cells.same_answer(found, want), (key, found, want)


def drops(answer: Any) -> bool:
    if isinstance(answer, str):
        return condition(answer) is not None
    return all(row == "null" or row.endswith("|null") for row in answer["rows"])


@pytest.mark.parametrize("key", KEYS, ids=[f"{cells.class_of(key)}:{key}" for key in KEYS])
def test_a_cell_answers_as_spark_or_as_its_named_class(
    measured: tuple[str, dict[str, Any], Path], key: str
) -> None:
    mode, found, _ = measured
    want = ORACLE["cells"][mode][key]
    if key in cells.KEPT_DIVERGENCE:
        assert drops(want), (key, want)
        exact = cells.kept_exact(key, ORACLE["cells"]["false"][key]["types"])
        assert cells.same_answer(found[key], exact), (key, found[key], exact)
        return
    if key in cells.CONDITION_CLASS and mode == "true":
        assert condition(want) == cells.CONDITION_CLASS[key], (key, want)
        assert isinstance(found[key], str), (key, found[key])
        assert condition(found[key]) == "ARITHMETIC_OVERFLOW", (key, found[key])
        return
    assert_as_spark(found[key], want, key)


def test_no_data_file_holds_a_sum_past_38_digits_or_its_wrap(
    measured: tuple[str, dict[str, Any], Path],
) -> None:
    _, _, warehouse = measured
    values = cells.data_values(warehouse)
    assert values, "the fitting groups were written"
    for value in values:
        sign, digits, exponent = value.as_tuple()
        unscaled = int("".join(map(str, digits))) * (-1 if sign else 1)
        assert abs(unscaled) < 10**38, value
        assert -exponent in (0, 38), value
    allowed = {Decimal(350), Decimal("3.50E-36")}
    assert {value for value in values if value != 0} <= allowed, values


def drops_the_total(answer: Any, mode: str, function: str, g: str) -> bool:
    if mode == "true" and function == "sum":
        return isinstance(answer, str) and condition(answer) == "ARITHMETIC_OVERFLOW"
    return isinstance(answer, dict) and answer["rows"] == [f"{g}|null"]


@pytest.mark.parametrize("mode", ["false", "true"])
def test_spark_drops_the_total_of_the_kept_divergence_group_on_every_run(mode: str) -> None:
    for key, runs in ORACLE["repeats"][mode].items():
        _, function, group, _ = key.split("/")
        if group[1:] in cells.MERGE_TOTAL_CLASS:
            assert len({json.dumps(run, sort_keys=True) for run in runs}) == 1, key
            assert drops_the_total(runs[0], mode, function, group[1:]), (key, runs[0])


@pytest.fixture(scope="module")
def spread_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("sum-spread")
    cells.write_spread_tables(root)
    return root


def spread_runs(root: Path, mode: str, partitions: int, keys: list[str]) -> dict[str, list[Any]]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-sum-dec-i128wrap-1-spread")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", mode)
        .getOrCreate()
    )
    try:
        session.sql(f"SET datafusion.execution.target_partitions = {partitions}").collect()
        cells.register_spread_tables(session, root)
        return {
            key: [
                cells.attempt(lambda key=key: cells.answer_of(session.sql(SPREAD[key])))
                for _ in range(cells.REPEATS)
            ]
            for key in keys
        }
    finally:
        session.stop()
        _reset_active_session_for_tests()


@pytest.mark.parametrize("mode", ["false", "true"], ids=["ansi_off", "ansi_on"])
@pytest.mark.parametrize("partitions", cells.TARGET_PARTITIONS, ids=lambda n: f"tp{n}")
@pytest.mark.parametrize(("table", "column"), SPREAD_SETS, ids=[f"{t}-{c}" for t, c in SPREAD_SETS])
def test_a_multi_partial_total_is_the_exact_total_on_every_run_at_2_4_and_8_partitions(
    spread_root: Path, mode: str, partitions: int, table: str, column: str
) -> None:
    keys = [
        key for key in sorted(SPREAD) if key.startswith(f"{table}/") and key.endswith(f"/{column}")
    ]
    for key, runs in spread_runs(spread_root, mode, partitions, keys).items():
        seen = {json.dumps(run, sort_keys=True) for run in runs}
        assert len(runs) == cells.REPEATS, key
        assert len(seen) == 1, (key, len(seen), sorted(text[:160] for text in seen))
        want = cells.spread_exact(key, mode)
        if isinstance(want, str):
            assert isinstance(runs[0], str), (key, str(runs[0])[:300])
            assert condition(runs[0]) == want, (key, runs[0][:300])
        else:
            assert cells.same_answer(runs[0], want), (key, str(runs[0])[:300], str(want)[:300])


OVER_A = 10**38 - 1
OVER_SETS = {
    "wrap5": [OVER_A, OVER_A, OVER_A, 2**128 + 5 - 3 * OVER_A],
    "neg": [-OVER_A, -OVER_A, -OVER_A, -(2**128 + 5 - 3 * OVER_A)],
    "fit": [OVER_A, OVER_A, -OVER_A, 7],
    "filler": [0],
}
OVER_FILES = [
    (name, p, files) for name in ("wrap5", "neg", "fit") for p in (19, 20) for files in (1, 2)
]
OVER_QUERIES = {
    ("sum", "ungrouped"): "SELECT sum(c) FROM t",
    ("try_sum", "ungrouped"): "SELECT try_sum(c) FROM t",
    ("sum", "grouped"): "SELECT g, sum(c) FROM t GROUP BY g",
    ("try_sum", "grouped"): "SELECT g, try_sum(c) FROM t GROUP BY g",
}


def write_past_precision(path: Path, values: list[int], precision: int) -> None:
    table = pa.table(
        {
            "c": pa.array([Decimal(v) for v in values], pa.decimal128(38, 0)),
            "g": pa.array([1] * len(values), pa.int64()),
        }
    )
    pq.write_table(table, path, store_schema=False, write_statistics=False)
    data = bytearray(path.read_bytes())
    size = struct.unpack("<I", data[-8:-4])[0]
    start = len(data) - 8 - size
    footer = bytes(data[start:-8])
    patched = footer.replace(b"\x15\x4c", bytes([0x15, precision * 2]))
    assert len(patched) == len(footer) and patched != footer
    data[start:-8] = patched
    path.write_bytes(bytes(data))
    back = pq.read_table(path)
    assert back.schema.field("c").type == pa.decimal128(precision, 0)
    assert [int(v) for v in back.column("c").to_pylist()] == values


@pytest.fixture(scope="module")
def past_precision_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("sum-past-precision")
    for name, values in OVER_SETS.items():
        for precision in (19, 20):
            write_past_precision(root / f"{name}_p{precision}.parquet", values, precision)
    for name, precision, files in OVER_FILES:
        if files == 2:
            two = root / f"{name}_p{precision}_two"
            two.mkdir()
            shutil.copy(root / f"{name}_p{precision}.parquet", two / "a.parquet")
            shutil.copy(root / f"filler_p{precision}.parquet", two / "b.parquet")
    return root


def past_precision_outcome(answer: Any) -> str:
    if isinstance(answer, dict) and answer["rows"] in (["null"], ["1|null"]):
        return "NULL"
    if isinstance(answer, str) and "past its declared precision" in answer:
        return "PAST_DECLARED_PRECISION"
    if isinstance(answer, str) and condition(answer) == "ARITHMETIC_OVERFLOW":
        return "ARITHMETIC_OVERFLOW"
    return f"a value or another error: {answer}"


def past_precision_want(
    name: str, precision: int, files: int, mode: str, partitions: int, query: tuple[str, str]
) -> str:
    function, shape = query
    multi_phase = partitions > 1 and (shape == "grouped" or files == 2)
    if multi_phase and precision == 19 and name != "fit":
        return "PAST_DECLARED_PRECISION"
    if mode == "true" and function == "sum":
        return "ARITHMETIC_OVERFLOW"
    return "NULL"


@pytest.mark.parametrize("mode", ["false", "true"], ids=["ansi_off", "ansi_on"])
@pytest.mark.parametrize("partitions", (1, 4), ids=lambda n: f"tp{n}")
@pytest.mark.parametrize(
    ("name", "precision", "files"),
    OVER_FILES,
    ids=[f"spark-refuses-the-read:{n}_p{p}-{f}-file" for n, p, f in OVER_FILES],
)
def test_a_total_of_values_past_their_declared_precision_is_never_a_value(
    past_precision_root: Path, mode: str, partitions: int, name: str, precision: int, files: int
) -> None:
    path = past_precision_root / (
        f"{name}_p{precision}_two" if files == 2 else f"{name}_p{precision}.parquet"
    )
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-sum-dec-i128wrap-1-past-precision")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", mode)
        .getOrCreate()
    )
    try:
        session.sql(f"SET datafusion.execution.target_partitions = {partitions}").collect()
        session.read.parquet(str(path)).createOrReplaceTempView("t")
        for query, sql in OVER_QUERIES.items():
            answer = cells.attempt(lambda sql=sql: cells.answer_of(session.sql(sql)))
            want = past_precision_want(name, precision, files, mode, partitions, query)
            assert past_precision_outcome(answer) == want, (query, str(answer)[:300])
    finally:
        session.stop()
        _reset_active_session_for_tests()
