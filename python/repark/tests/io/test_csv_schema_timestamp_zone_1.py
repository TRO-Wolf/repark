from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("csv_schema_timestamp_zone_1")
LA = "America/Los_Angeles"
TK = "Asia/Tokyo"
UTC = "UTC"
SCHEMA = "n INT, ts TIMESTAMP"
UTC_PARSE = [(1, 1704078000), (2, 1704110400)]
LA_PARSE = [(1, 1704106800), (2, 1704139200)]
SPARK_BY_ZONE = {UTC: UTC_PARSE, LA: LA_PARSE}
GAP = "2024-03-10 02:30:00"
FILES = {
    "base": "1,2024-01-01 03:00:00\n2,2024-01-01 12:00:00\n",
    "dst": "1,2024-03-10 02:30:00\n2,2024-11-03 01:30:00\n",
    "iso": "1,2024-01-01T03:00:00\n2,2024-06-01T23:30:00\n",
    "hdr": "n,ts\n1,2024-01-01 03:00:00\n2,2024-06-01 23:30:00\n",
    "hdrpos": "a,b\n1,2024-01-01 03:00:00\n2,2024-06-01 23:30:00\n",
    "twots": "1,2024-01-01 03:00:00,2024-06-01 23:30:00\n",
    "emptyts": "1,\n2,2024-01-01 03:00:00\n",
    "dexp": "1,2024-01-01 03:00:00,1e2\n2,2024-01-01 12:00:00,1.5E-1\n",
    "ntzbeside": f"1,{GAP},2024-01-01 03:00:00\n2,2024-01-01 03:00:00,{GAP}\n",
    "ntzempty": f"1,{GAP},\n2,2024-01-01 03:00:00,\n",
    "ntzz": f"1,{GAP},2024-01-01T03:00:00Z\n2,2024-01-01 03:00:00,2024-06-01T00:00:00Z\n",
}
U = "SELECT n, unix_timestamp(ts) AS u FROM shape"
SHAPES: dict[str, tuple[Callable[[Any, Path], Any], str, dict[str, list[tuple]]]] = {
    "dst": (
        lambda read, root: read.schema(SCHEMA).csv(str(root / "dst")),
        U,
        {LA: [(1, 1710066600), (2, 1730622600)], TK: [(1, 1710005400), (2, 1730565000)]},
    ),
    "iso": (
        lambda read, root: read.schema(SCHEMA).csv(str(root / "iso")),
        U,
        {LA: [(1, 1704106800), (2, 1717309800)], TK: [(1, 1704045600), (2, 1717252200)]},
    ),
    "header": (
        lambda read, root: read.option("header", "true").schema(SCHEMA).csv(str(root / "hdr")),
        U,
        {LA: [(1, 1704106800), (2, 1717309800)], TK: [(1, 1704045600), (2, 1717252200)]},
    ),
    "header_by_position": (
        lambda read, root: read.option("header", "true").schema(SCHEMA).csv(str(root / "hdrpos")),
        U,
        {LA: [(1, 1704106800), (2, 1717309800)], TK: [(1, 1704045600), (2, 1717252200)]},
    ),
    "two_fields": (
        lambda read, root: read.schema("n INT, a TIMESTAMP, b TIMESTAMP").csv(str(root / "twots")),
        "SELECT n, unix_timestamp(a), unix_timestamp(b) FROM shape",
        {LA: [(1, 1704106800, 1717309800)], TK: [(1, 1704045600, 1717252200)]},
    ),
    "empty_field": (
        lambda read, root: read.schema(SCHEMA).csv(str(root / "emptyts")),
        U,
        {LA: [(1, None), (2, 1704106800)], TK: [(1, None), (2, 1704045600)]},
    ),
}
NTZ = "n INT, a TIMESTAMP_NTZ, b TIMESTAMP"
NTZ_QUERY = "SELECT n, CAST(a AS STRING), unix_timestamp(b) FROM shape"
T3 = "2024-01-01 03:00:00"
DELTA_1: dict[str, tuple[str, str, str, dict[str, list[tuple]]]] = {
    "decimal_exponent": (
        "dexp",
        "n INT, ts TIMESTAMP, d DECIMAL(10,2)",
        "SELECT n, unix_timestamp(ts), CAST(d AS STRING) FROM shape",
        {
            UTC: [(1, 1704078000, "100.00"), (2, 1704110400, "0.15")],
            LA: [(1, 1704106800, "100.00"), (2, 1704139200, "0.15")],
        },
    ),
    "ntz_beside_timestamp": (
        "ntzbeside",
        NTZ,
        NTZ_QUERY,
        {
            UTC: [(1, GAP, 1704078000), (2, T3, 1710037800)],
            LA: [(1, GAP, 1704106800), (2, T3, 1710066600)],
        },
    ),
    "ntz_beside_empty_timestamp": (
        "ntzempty",
        NTZ,
        NTZ_QUERY,
        {zone: [(1, GAP, None), (2, T3, None)] for zone in (UTC, LA)},
    ),
    "ntz_beside_zoned_timestamp": (
        "ntzz",
        NTZ,
        NTZ_QUERY,
        {zone: [(1, GAP, 1704078000), (2, T3, 1717200000)] for zone in (UTC, LA)},
    ),
}
GLOB_SCHEMA = "n INT, d DECIMAL(10,2), tn TIMESTAMP_NTZ, ts TIMESTAMP"
GLOB_TEXT_A = "1,1.25,2024-01-02 03:04:05,2024-06-01 12:00:00\n"
GLOB_TEXT_B = "2,2.50,2024-02-03 04:05:06,2023-12-31 23:00:00\n"
GLOB_A = {UTC: (1, 1717243200), LA: (1, 1717268400)}
GLOB_B = {UTC: (2, 1704063600), LA: (2, 1704092400)}
FIVE_NAMES = ("a b.csv", "a#b.csv", "a%b.csv", "a%41b.csv")
GLOBS: dict[str, tuple[str, dict[str, str], list[dict[str, tuple]]]] = {
    "literal_star_file": (
        "gl/a*b.csv",
        {"gl/a*b.csv": GLOB_TEXT_A, "gl/axb.csv": GLOB_TEXT_B},
        [GLOB_A, GLOB_B],
    ),
    "literal_question_file": (
        "gq/a?.csv",
        {"gq/a?.csv": GLOB_TEXT_A, "gq/ab.csv": GLOB_TEXT_B},
        [GLOB_A, GLOB_B],
    ),
    "literal_bracket_file": (
        "gb/a[1].csv",
        {"gb/a[1].csv": GLOB_TEXT_A, "gb/a1.csv": GLOB_TEXT_B},
        [GLOB_B],
    ),
    "literal_star_directory": (
        "dd*",
        {"dd*/part.csv": GLOB_TEXT_A, "ddx/part.csv": GLOB_TEXT_B},
        [GLOB_A, GLOB_B],
    ),
    "five_file_star_match": (
        "n5/a*b.csv",
        {"n5/a*b.csv": GLOB_TEXT_A, **{f"n5/{name}": GLOB_TEXT_B for name in FIVE_NAMES}},
        [GLOB_A, GLOB_B, GLOB_B, GLOB_B, GLOB_B],
    ),
}
WRITES: dict[str, Callable[[Any, str], None]] = {
    "ctas": lambda session, table: session.sql(
        f"CREATE TABLE {table} USING iceberg AS SELECT * FROM made_utc"
    ).collect(),
    "insert": lambda session, table: session.sql(
        f"INSERT INTO {table} SELECT * FROM made_utc"
    ).collect(),
    "write_to": lambda session, table: (
        session.table("made_utc").writeTo(table).using("iceberg").create()
    ),
    "insert_into": lambda session, table: session.table("made_utc").write.insertInto(table),
}


def conf(session: Any, zone: str) -> None:
    session.sql(f"SET spark.sql.session.timeZone={zone}").collect()


def rows(session: Any, query: str) -> list[tuple]:
    return sorted(tuple(row) for row in session.sql(query).collect())


def read_base(session: Any, root: Path, reader: str) -> Any:
    path = str(root / "base")
    if reader == "csv_s":
        return session.read.schema(SCHEMA).csv(path)
    return session.read.format("csv").schema(SCHEMA).load(path)


def measure_matching(session: Any, root: Path, found: dict[str, Any]) -> None:
    for reader in ("csv_s", "csv_f"):
        for zone in (UTC, LA):
            conf(session, zone)
            frame = read_base(session, root, reader)
            frame.createOrReplaceTempView("matching")
            found[f"matching/{reader}/{zone}"] = (
                frame.dtypes,
                rows(session, "SELECT n, unix_timestamp(ts) FROM matching"),
            )


def measure_views(session: Any, root: Path, found: dict[str, Any]) -> None:
    for made, read in ((LA, UTC), (UTC, LA)):
        conf(session, made)
        read_base(session, root, "csv_s").createOrReplaceTempView("made")
        session.sql("CREATE OR REPLACE TEMP VIEW made_sql AS SELECT * FROM made").collect()
        conf(session, read)
        for door, view in (("frame_view", "made"), ("sql_view", "made_sql")):
            query = f"SELECT n, unix_timestamp(ts) FROM {view}"
            found[f"view/{door}/{made}/{read}"] = rows(session, query)


def measure_writes(session: Any, root: Path, found: dict[str, Any]) -> None:
    conf(session, UTC)
    read_base(session, root, "csv_s").createOrReplaceTempView("made_utc")
    session.sql("CREATE NAMESPACE IF NOT EXISTS lc.w").collect()
    for write, action in WRITES.items():
        table = f"lc.w.{write}"
        if write in ("insert", "insert_into"):
            session.sql(
                f"CREATE TABLE {table} USING iceberg AS SELECT * FROM made_utc WHERE false"
            ).collect()
        conf(session, LA)
        action(session, table)
        conf(session, UTC)
        found[f"write/{write}"] = rows(session, f"SELECT n, unix_timestamp(ts) FROM {table}")


def measure_shapes(session: Any, root: Path, found: dict[str, Any]) -> None:
    for shape, (build, query, _) in SHAPES.items():
        for zone in (LA, TK):
            conf(session, zone)
            build(session.read, root).createOrReplaceTempView("shape")
            found[f"shape/{shape}/{zone}"] = rows(session, query)


def measure_delta_1(session: Any, root: Path, found: dict[str, Any]) -> None:
    for shape, (name, schema, query, _) in DELTA_1.items():
        for zone in (UTC, LA):
            conf(session, zone)
            try:
                frame = session.read.schema(schema).csv(str(root / name))
                frame.createOrReplaceTempView("shape")
                found[f"delta_1/{shape}/{zone}"] = rows(session, query)
            except Exception as error:
                found[f"delta_1/{shape}/{zone}"] = type(error).__name__


def measure_globs(session: Any, root: Path, found: dict[str, Any]) -> None:
    for shape, (spelling, files, _) in GLOBS.items():
        for name, text in files.items():
            target = root / "glob" / shape / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
        for zone in (UTC, LA):
            conf(session, zone)
            frame = session.read.schema(GLOB_SCHEMA).csv(str(root / "glob" / shape / spelling))
            frame.createOrReplaceTempView("glob")
            found[f"glob/{shape}/{zone}"] = rows(session, "SELECT n, unix_timestamp(ts) FROM glob")


@pytest.fixture(scope="module", params=["true", "false"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp(f"csv-ts-zone-{request.param}")
    for name, text in FILES.items():
        (root / name).mkdir()
        (root / name / "a.csv").write_text(text)
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-csv-schema-timestamp-zone-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "lc"))
        .config("spark.sql.session.timeZone", UTC)
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    found: dict[str, Any] = {}
    steps = (
        measure_matching,
        measure_views,
        measure_writes,
        measure_shapes,
        measure_delta_1,
        measure_globs,
    )
    for step in steps:
        step(session, root, found)
    session.stop()
    _reset_active_session_for_tests()
    yield found


@pytest.mark.parametrize("reader", ["csv_s", "csv_f"])
def test_a_schema_timestamp_parses_under_the_session_zone(
    measured: dict[str, Any], reader: str
) -> None:
    for zone in (UTC, LA):
        dtypes, got = measured[f"matching/{reader}/{zone}"]
        assert dtypes == [("n", "int"), ("ts", "timestamp")], zone
        assert got == SPARK_BY_ZONE[zone], zone


@pytest.mark.parametrize("door", ["frame_view", "sql_view"])
def test_a_view_read_in_another_zone_parses_under_the_reading_zone(
    measured: dict[str, Any], door: str
) -> None:
    for made, read in ((LA, UTC), (UTC, LA)):
        assert measured[f"view/{door}/{made}/{read}"] == SPARK_BY_ZONE[read], (made, read)


@pytest.mark.parametrize("write", sorted(WRITES))
def test_a_write_in_another_zone_stores_the_writing_zone_parse(
    measured: dict[str, Any], write: str
) -> None:
    assert measured[f"write/{write}"] == LA_PARSE


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_every_schema_timestamp_shape_parses_under_the_session_zone(
    measured: dict[str, Any], shape: str
) -> None:
    for zone in (LA, TK):
        assert measured[f"shape/{shape}/{zone}"] == SHAPES[shape][2][zone], zone


@pytest.mark.parametrize("shape", sorted(DELTA_1))
def test_a_schema_timestamp_field_leaves_the_other_fields_on_the_typed_scan(
    measured: dict[str, Any], shape: str
) -> None:
    for zone in (UTC, LA):
        assert measured[f"delta_1/{shape}/{zone}"] == DELTA_1[shape][3][zone], zone


@pytest.mark.parametrize("shape", sorted(GLOBS))
def test_a_literal_name_holding_a_glob_character_reads_every_file_its_glob_matches(
    measured: dict[str, Any], shape: str
) -> None:
    for zone in (UTC, LA):
        expected = sorted(row[zone] for row in GLOBS[shape][2])
        assert measured[f"glob/{shape}/{zone}"] == expected, zone
