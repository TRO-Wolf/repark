from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import Callable
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    UnsupportedOperationException,
)

CARD = "PLAIN-DYNAMIC-OVERWRITE-GUARD-1"
KEY = "spark.sql.sources.partitionOverwriteMode"
SEED = [(1, "x", 10), (1, "y", 20), (2, "x", 30)]
SEED_ROWS = [("a=1/b=x", "10"), ("a=1/b=y", "20"), ("a=2/b=x", "30")]
ONE = [(1, "x", 40)]
COMPOSITE = [(1, "x", 40), (3, "z", 50)]
COVER = [(1, "x", 41), (1, "y", 42), (2, "x", 43)]
SPELLINGS = ("partitionBy-list", "shorthand-keywords", "save-keywords", "option-path")
KEYS = ["a", "b"]
FORMATS = ("parquet", "json", "csv", "text")
ROUTES = ("shorthand", "save")
SPARK_SAYS = "Spark would replace only the partitions the written rows hold"


def _frame(spark: ReparkSession, fmt: str, keys: list[str], rows: list[tuple[Any, ...]]) -> Any:
    if fmt == "text":
        data = [(a, b, str(v)) for a, b, v in rows]
        return spark.createDataFrame(data, "a INT, b STRING, v STRING").select(*keys, "v")
    return spark.createDataFrame(rows, "a INT, b STRING, v BIGINT")


def _write(writer: Any, fmt: str, route: str, target: Path | str) -> None:
    if route == "save":
        writer.format(fmt).save(str(target))
    else:
        getattr(writer, fmt)(str(target))


def _seed(spark: ReparkSession, fmt: str, target: Path, keys: list[str] = KEYS) -> None:
    writer = _frame(spark, fmt, keys, SEED).write
    if keys:
        writer = writer.partitionBy(*keys)
    getattr(writer, fmt)(str(target))


def _tree(base: Path) -> list[tuple[str, str]]:
    entries = []
    for entry in sorted(base.rglob("*")):
        rel = entry.relative_to(base).as_posix()
        digest = "dir" if entry.is_dir() else hashlib.sha256(entry.read_bytes()).hexdigest()
        entries.append((rel, digest))
    return entries


def _values(fmt: str, file: Path) -> list[str]:
    if fmt == "parquet":
        return [str(value) for value in pq.read_table(file).column("v").to_pylist()]
    lines = [line for line in file.read_text().splitlines() if line]
    if fmt == "json":
        return [str(json.loads(line)["v"]) for line in lines]
    if fmt == "csv":
        parsed = csv.reader(io.StringIO("\n".join(lines)))
        return [row[-1] for row in parsed if row not in (["v"], ["a", "b", "v"])]
    return lines


def _rows(fmt: str, target: Path) -> list[tuple[str, str]]:
    found = []
    for file in sorted(target.rglob("*")):
        if file.is_dir() or file.name.startswith((".", "_")):
            continue
        part = file.parent.relative_to(target).as_posix()
        found.extend((part, value) for value in _values(fmt, file))
    return sorted(found)


def _writer(
    spark: ReparkSession,
    fmt: str,
    rows: list[tuple[Any, ...]],
    *,
    mode: str = "overwrite",
    keys: list[str] = KEYS,
    options: tuple[tuple[str, str], ...] = (),
) -> Any:
    writer = _frame(spark, fmt, keys, rows).write.mode(mode)
    if keys:
        writer = writer.partitionBy(*keys)
    for key, value in options:
        writer = writer.option(key, value)
    return writer


def _refusal(action: Callable[[], None], base: Path) -> Exception | None:
    before = _tree(base)
    raised: Exception | None = None
    try:
        action()
    except (AnalysisException, IllegalArgumentException, UnsupportedOperationException) as error:
        raised = error
    assert _tree(base) == before, f"the write changed the scratch tree (raised {raised!r})"
    return raised


def _assert_refused_intact(writer: Any, fmt: str, route: str, base: Path) -> None:
    raised = _refusal(lambda: _write(writer, fmt, route, base / "t"), base)
    assert isinstance(raised, UnsupportedOperationException), repr(raised)
    message = str(raised)
    assert CARD in message
    assert SPARK_SAYS in message
    assert "refuses rather than overwrite every partition" in message


@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_composite_overwrite_refuses_with_seed_bytes_intact(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, ansi: bool
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "dynamic")
    _assert_refused_intact(_writer(spark, fmt, COMPOSITE), fmt, route, tmp_path)
    assert _rows(fmt, tmp_path / "t") == SEED_ROWS


@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_empty_overwrite_refuses_and_keeps_every_row(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, ansi: bool
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "DYNAMIC")
    _assert_refused_intact(_writer(spark, fmt, []), fmt, route, tmp_path)
    assert _rows(fmt, tmp_path / "t") == SEED_ROWS


@pytest.mark.parametrize(
    "options",
    [
        (("partitionOverwriteMode", "DYNAMIC"),),
        (("PartitionOverWriteMODE", "DyNaMiC"),),
        (("partitionOverwriteMode", "static"), ("PARTITIONOVERWRITEMODE", "dynamic")),
    ],
    ids=["option-dynamic", "mixed-case", "last-spelling-wins"],
)
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_writer_option_refuses_over_a_static_session(
    spark: ReparkSession,
    tmp_path: Path,
    fmt: str,
    route: str,
    options: tuple[tuple[str, str], ...],
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "static")
    writer = _writer(spark, fmt, ONE, options=options)
    _assert_refused_intact(writer, fmt, route, tmp_path)


@pytest.mark.parametrize("how", ["conf", "sql"])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_set_after_the_writer_is_built_refuses(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, how: str
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "static")
    writer = _writer(spark, fmt, ONE)
    if how == "sql":
        spark.sql(f"SET {KEY}=dynamic").collect()
    else:
        spark.conf.set(KEY, "dynamic")
    _assert_refused_intact(writer, fmt, route, tmp_path)


@pytest.mark.parametrize("rows", [ONE, []], ids=["one-row", "empty"])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_overwrite_of_an_absent_target_leaves_it_absent(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, rows: list[tuple[Any, ...]]
) -> None:
    spark.conf.set(KEY, "dynamic")
    _assert_refused_intact(_writer(spark, fmt, rows), fmt, route, tmp_path)
    assert not (tmp_path / "t").exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("raw", ["bogus", "", " dynamic"])
@pytest.mark.parametrize("fmt", FORMATS)
def test_an_unparsable_option_refuses_a_partitioned_overwrite(
    spark: ReparkSession, tmp_path: Path, fmt: str, raw: str
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    writer = _writer(spark, fmt, ONE, options=(("partitionOverwriteMode", raw),))
    raised = _refusal(lambda: _write(writer, fmt, "shorthand", tmp_path / "t"), tmp_path)
    assert isinstance(raised, IllegalArgumentException), repr(raised)
    assert CARD in str(raised)


@pytest.mark.parametrize("ansi", [True, False])
def test_witness_setting_residual_partition_overwrite_mode(
    spark: ReparkSession, tmp_path: Path, ansi: bool
) -> None:
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    spark.conf.set(KEY, "DYNAMIC")
    target = str(tmp_path / "t")
    spark.sql("SELECT * FROM VALUES (0,10),(1,11) t(part,v)").write.partitionBy("part").parquet(
        target
    )
    fresh = spark.sql("SELECT 1 AS part, 21 AS v").write.mode("overwrite").partitionBy("part")
    raised = _refusal(lambda: fresh.parquet(target), tmp_path)
    assert isinstance(raised, UnsupportedOperationException), repr(raised)
    assert CARD in str(raised)
    assert sorted(
        (row["part"], row["v"]) for row in pq.read_table(target, partitioning="hive").to_pylist()
    ) == [(0, 10), (1, 11)]


def _witness_frames(spark: ReparkSession, target: Path) -> Any:
    seed = spark.createDataFrame([(0, 10), (1, 11), (2, 12)], "part INT, v BIGINT")
    seed.write.partitionBy("part").parquet(str(target))
    fresh = spark.createDataFrame([(1, 21), (3, 23)], "part INT, v BIGINT")
    return fresh


def _partition_rows(target: Path) -> list[tuple[str, list[int]]]:
    out = []
    for directory in sorted(target.glob("part=*")):
        values = []
        for file in sorted(directory.rglob("*.parquet")):
            values.extend(pq.ParquetFile(file).read().column("v").to_pylist())
        out.append((directory.name, sorted(values)))
    return out


def test_witness_review_settings_overwrite_dynamic(spark: ReparkSession, tmp_path: Path) -> None:
    spark.conf.set(KEY, "DYNAMIC")
    fresh = _witness_frames(spark, tmp_path / "t")
    writer = fresh.write.mode("overwrite").partitionBy("part")
    raised = _refusal(lambda: writer.parquet(str(tmp_path / "t")), tmp_path)
    assert isinstance(raised, UnsupportedOperationException), repr(raised)
    assert CARD in str(raised)


def test_witness_review_settings_overwrite_static(spark: ReparkSession, tmp_path: Path) -> None:
    spark.conf.set(KEY, "STATIC")
    fresh = _witness_frames(spark, tmp_path / "t")
    fresh.write.mode("overwrite").partitionBy("part").parquet(str(tmp_path / "t"))
    assert _partition_rows(tmp_path / "t") == [("part=1", [21]), ("part=3", [23])]


def test_witness_review_settings_overwrite_append(spark: ReparkSession, tmp_path: Path) -> None:
    spark.conf.set(KEY, "DYNAMIC")
    fresh = _witness_frames(spark, tmp_path / "t")
    fresh.write.mode("append").partitionBy("part").parquet(str(tmp_path / "t"))
    assert _partition_rows(tmp_path / "t") == [
        ("part=0", [10]),
        ("part=1", [11, 21]),
        ("part=2", [12]),
        ("part=3", [23]),
    ]


@pytest.mark.parametrize("fmt", ["parquet", "json", "csv"])
def test_s3_dispatch_is_never_reached_on_a_dynamic_overwrite(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch, fmt: str
) -> None:
    from repark.spark.dataframe import writer_s3

    handed: list[Any] = []
    monkeypatch.setattr(
        writer_s3,
        "_native",
        SimpleNamespace(session_write_path=lambda *arguments: handed.append(arguments)),
    )
    spark.conf.set(KEY, "dynamic")
    with pytest.raises(UnsupportedOperationException, match=CARD):
        _write(_writer(spark, fmt, ONE), fmt, "shorthand", "s3a://guard-bucket/t")
    assert handed == []
    spark.conf.set(KEY, "static")
    _write(_writer(spark, fmt, ONE), fmt, "shorthand", "s3a://guard-bucket/t")
    assert len(handed) == 1


def test_s3_text_refuses_dynamic_before_the_local_only_refusal(spark: ReparkSession) -> None:
    spark.conf.set(KEY, "dynamic")
    with pytest.raises(UnsupportedOperationException, match=CARD):
        _writer(spark, "text", ONE).text("s3a://guard-bucket/t")
    spark.conf.set(KEY, "static")
    with pytest.raises(AnalysisException, match="local only"):
        _writer(spark, "text", ONE).text("s3a://guard-bucket/t")


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_static_composite_overwrite_still_replaces_the_path(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "static")
    _write(_writer(spark, fmt, COMPOSITE), fmt, route, tmp_path / "t")
    assert _rows(fmt, tmp_path / "t") == [("a=1/b=x", "40"), ("a=3/b=z", "50")]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_static_empty_overwrite_still_empties_the_path(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "static")
    _write(_writer(spark, fmt, []), fmt, route, tmp_path / "t")
    assert _rows(fmt, tmp_path / "t") == []


@pytest.mark.parametrize("option", [False, True], ids=["session", "option"])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_unpartitioned_overwrite_still_replaces_the_path(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, option: bool
) -> None:
    _seed(spark, fmt, tmp_path / "t", keys=[])
    spark.conf.set(KEY, "static" if option else "dynamic")
    options = (("partitionOverwriteMode", "dynamic"),) if option else ()
    writer = _writer(spark, fmt, ONE, keys=[], options=options)
    if option and fmt != "text":
        with pytest.raises(AnalysisException, match="not supported yet"):
            _write(writer, fmt, route, tmp_path / "t")
        assert _rows(fmt, tmp_path / "t") == [(".", "10"), (".", "20"), (".", "30")]
        return
    _write(writer, fmt, route, tmp_path / "t")
    assert _rows(fmt, tmp_path / "t") == [(".", "40")]


@pytest.mark.parametrize("rows", [ONE, []], ids=["one-row", "empty"])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_append_still_appends(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, rows: list[tuple[Any, ...]]
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "dynamic")
    _write(_writer(spark, fmt, rows, mode="append"), fmt, route, tmp_path / "t")
    added = [("a=1/b=x", "40")] if rows else []
    assert _rows(fmt, tmp_path / "t") == sorted(SEED_ROWS + added)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_ignore_and_error_keep_their_branches(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "dynamic")
    before = _tree(tmp_path)
    _write(_writer(spark, fmt, ONE, mode="ignore"), fmt, route, tmp_path / "t")
    assert _tree(tmp_path) == before
    with pytest.raises(AnalysisException, match="PATH_ALREADY_EXISTS"):
        _write(_writer(spark, fmt, ONE, mode="error"), fmt, route, tmp_path / "t")
    assert _tree(tmp_path) == before


@pytest.mark.parametrize("set_after", [False, True], ids=["option", "set-after"])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_static_intent_over_a_dynamic_session_is_not_refused_by_the_guard(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, set_after: bool
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "dynamic")
    options = () if set_after else (("partitionOverwriteMode", "STATIC"),)
    writer = _writer(spark, fmt, ONE, options=options)
    if set_after:
        spark.conf.set(KEY, "static")
    if options and fmt != "text":
        before = _tree(tmp_path)
        with pytest.raises(AnalysisException, match="not supported yet") as raised:
            _write(writer, fmt, route, tmp_path / "t")
        assert CARD not in str(raised.value)
        assert _tree(tmp_path) == before
        return
    _write(writer, fmt, route, tmp_path / "t")
    assert _rows(fmt, tmp_path / "t") == [("a=1/b=x", "40")]


def _spelled_write(frame: Any, fmt: str, spelling: str, target: Path) -> None:
    path = str(target)
    if spelling == "partitionBy-list":
        getattr(frame.write.mode("overwrite").partitionBy(list(KEYS)), fmt)(path)
    elif spelling == "shorthand-keywords":
        getattr(frame.write, fmt)(path, mode="overwrite", partitionBy=list(KEYS))
    elif spelling == "save-keywords":
        frame.write.save(path, format=fmt, mode="overwrite", partitionBy=list(KEYS))
    else:
        writer = frame.write.format(fmt).mode("overwrite").partitionBy(*KEYS)
        writer.option("path", path).save()


@pytest.mark.parametrize(
    "rows", [COVER, [*COVER, (3, "z", 44)]], ids=["covers-every-partition", "covers-plus-new"]
)
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_overwrite_covering_every_partition_refuses_too(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, rows: list[tuple[Any, ...]]
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "dynamic")
    _assert_refused_intact(_writer(spark, fmt, rows), fmt, route, tmp_path)
    assert _rows(fmt, tmp_path / "t") == SEED_ROWS


@pytest.mark.parametrize("rows", [ONE, []], ids=["one-row", "empty"])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_overwrite_of_an_empty_directory_refuses_too(
    spark: ReparkSession, tmp_path: Path, fmt: str, route: str, rows: list[tuple[Any, ...]]
) -> None:
    (tmp_path / "t").mkdir()
    spark.conf.set(KEY, "dynamic")
    _assert_refused_intact(_writer(spark, fmt, rows), fmt, route, tmp_path)
    assert list((tmp_path / "t").iterdir()) == []


@pytest.mark.parametrize(
    ("fmt", "spelling"),
    [
        (fmt, spelling)
        for fmt in FORMATS
        for spelling in SPELLINGS
        if not (fmt == "text" and spelling == "shorthand-keywords")
    ],
)
def test_other_spellings_of_a_dynamic_partitioned_overwrite_refuse(
    spark: ReparkSession, tmp_path: Path, fmt: str, spelling: str
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "dynamic")
    frame = _frame(spark, fmt, KEYS, ONE)
    raised = _refusal(partial(_spelled_write, frame, fmt, spelling, tmp_path / "t"), tmp_path)
    assert isinstance(raised, UnsupportedOperationException), repr(raised)
    assert CARD in str(raised)


@pytest.mark.parametrize("fmt", FORMATS)
def test_dynamic_set_at_session_build_refuses_and_unset_restores_static(
    tmp_path: Path, fmt: str
) -> None:
    session = (
        ReparkSession.builder.appName("pdog-builder-conf").config(KEY, "dynamic").getOrCreate()
    )
    try:
        _seed(session, fmt, tmp_path / "t")
        _assert_refused_intact(_writer(session, fmt, ONE), fmt, "shorthand", tmp_path)
        session.conf.unset(KEY)
        _write(_writer(session, fmt, ONE), fmt, "shorthand", tmp_path / "t")
        assert _rows(fmt, tmp_path / "t") == [("a=1/b=x", "40")]
    finally:
        session.stop()


@pytest.mark.parametrize("fmt", FORMATS)
def test_unset_after_a_dynamic_conf_set_restores_the_static_replace(
    spark: ReparkSession, tmp_path: Path, fmt: str
) -> None:
    _seed(spark, fmt, tmp_path / "t")
    spark.conf.set(KEY, "dynamic")
    spark.conf.unset(KEY)
    _write(_writer(spark, fmt, ONE), fmt, "shorthand", tmp_path / "t")
    assert _rows(fmt, tmp_path / "t") == [("a=1/b=x", "40")]


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_new_session_writes_under_its_own_mode(
    spark: ReparkSession, tmp_path: Path, fmt: str
) -> None:
    child = spark.newSession()
    _seed(child, fmt, tmp_path / "t")
    child.conf.set(KEY, "dynamic")
    _assert_refused_intact(_writer(child, fmt, ONE), fmt, "shorthand", tmp_path)
    spark.conf.set(KEY, "dynamic")
    child.conf.set(KEY, "static")
    _write(_writer(child, fmt, ONE), fmt, "shorthand", tmp_path / "t")
    assert _rows(fmt, tmp_path / "t") == [("a=1/b=x", "40")]
